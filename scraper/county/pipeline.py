#!/usr/bin/env python3
"""
County-Level Law Firm Data Pipeline

Discovers and enriches law firm data for a specific county, outputting a
detailed CSV file. Uses Google Places API + Foursquare API for discovery,
then enhances with free legal directory sources and website scraping.

Usage:
    python -m scraper.county.pipeline --county johnson
    python -m scraper.county.pipeline --county johnson --test
    python -m scraper.county.pipeline --county johnson --skip-foursquare
    python -m scraper.county.pipeline --county johnson --skip-enhance
    python -m scraper.county.pipeline --county johnson --resume
"""
import argparse
import os
from collections import Counter
from dotenv import load_dotenv

from scraper.county.config import get_county_config
from scraper.county.google_places import discover_google
from scraper.county.foursquare import discover_foursquare
from scraper.county.enhance import enhance_firms
from scraper.county.csv_output import firms_to_csv
from scraper.county.manifest import update_manifest
from scraper.utils.normalize import deduplicate_firms, are_same_firm
from scraper.utils.checkpoint import save_checkpoint, load_checkpoint, clear_checkpoint

COUNTY_DATA_DIR = os.path.join("app", "county-data")


def _merge_foursquare(firms: list, fsq_firms: list) -> list:
    for fsq in fsq_firms:
        fsq_city = (fsq.get("address") or {}).get("city", "").lower()
        matched = False

        for firm in firms:
            firm_city = (firm.get("address") or {}).get("city", "").lower()
            if firm_city == fsq_city and are_same_firm(fsq["name"], firm["name"]):
                if not firm.get("phone") and fsq.get("phone"):
                    firm["phone"] = fsq["phone"]
                if not firm.get("website") and fsq.get("website"):
                    firm["website"] = fsq["website"]
                if not firm.get("coordinates") and fsq.get("coordinates"):
                    firm["coordinates"] = fsq["coordinates"]
                addr = firm.get("address") or {}
                fsq_addr = fsq.get("address") or {}
                if not addr.get("street") and fsq_addr.get("street"):
                    addr["street"] = fsq_addr["street"]
                if not addr.get("zip") and fsq_addr.get("zip"):
                    addr["zip"] = fsq_addr["zip"]
                sources = firm.setdefault("sources", [])
                if "foursquare" not in sources:
                    sources.append("foursquare")
                matched = True
                break

        if not matched:
            firms.append(fsq)

    return firms


def _checkpoint_path(county_slug: str) -> str:
    return os.path.join("data", "county", f"{county_slug}_checkpoint.json")


def _print_summary(firms: list, county_config: dict):
    source_counts = Counter()
    for f in firms:
        for s in f.get("sources", []):
            source_counts[s] += 1

    has_website = sum(1 for f in firms if f.get("website"))
    has_gbp = sum(1 for f in firms if f.get("google_business_profile"))
    has_directory = sum(1 for f in firms if
                       f.get("martindale_url") or f.get("justia_url") or
                       f.get("avvo_url") or f.get("findlaw_url"))
    has_phone = sum(1 for f in firms if f.get("phone"))
    has_email = sum(1 for f in firms if f.get("email"))
    has_practice = sum(1 for f in firms if f.get("practiceAreas"))

    print(f"\n{'=' * 55}")
    print(f"  County Scrape Complete: {county_config['name']}, {county_config['state']}")
    print(f"{'=' * 55}")
    print(f"  Total firms: {len(firms)}")
    print(f"\n  Data sources:")
    for src, count in source_counts.most_common():
        print(f"    {src}: {count} firms")
    print(f"\n  Coverage:")
    print(f"    With website:           {has_website}/{len(firms)}")
    print(f"    With Google profile:    {has_gbp}/{len(firms)}")
    print(f"    With directory listing: {has_directory}/{len(firms)}")
    print(f"    With phone:             {has_phone}/{len(firms)}")
    print(f"    With email:             {has_email}/{len(firms)}")
    print(f"    With practice area:     {has_practice}/{len(firms)}")
    print(f"{'=' * 55}")


def main():
    parser = argparse.ArgumentParser(description="County-Level Law Firm Data Pipeline")
    parser.add_argument("--county", required=True,
                        help="County key (e.g., 'johnson')")
    parser.add_argument("--skip-foursquare", action="store_true",
                        help="Skip Foursquare discovery phase")
    parser.add_argument("--skip-enhance", action="store_true",
                        help="Skip enhancement pass")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from last checkpoint")
    parser.add_argument("--test", action="store_true",
                        help="Test mode: limit results per phase")
    args = parser.parse_args()

    load_dotenv("scraper/.env")

    county_config = get_county_config(args.county)
    slug = county_config["slug"]
    cp_path = _checkpoint_path(slug)

    print(f"\n{'=' * 55}")
    print(f"  County Pipeline: {county_config['name']}, {county_config['state']}")
    print(f"  Cities: {len(county_config['cities'])}")
    print(f"  MSA: {county_config['msa']}")
    print(f"{'=' * 55}\n")

    if args.resume:
        checkpoint = load_checkpoint(cp_path)
    else:
        checkpoint = None

    if checkpoint:
        firms = checkpoint["firms"]
        start_stage = checkpoint["phase"]
        print(f"[checkpoint] Resuming from stage {start_stage} with {len(firms)} firms\n")
    else:
        firms = []
        start_stage = 1

    # ── Stage 1: Google Places Discovery ──
    if start_stage <= 1:
        google_key = os.getenv("GOOGLE_MAPS_API_KEY")
        if not google_key:
            print("[stage 1] WARNING: GOOGLE_MAPS_API_KEY not found — skipping Google Places")
        else:
            print("[stage 1] Google Places API discovery...")
            firms = discover_google(county_config, google_key, test_mode=args.test)
            print(f"[stage 1] Discovered {len(firms)} firms\n")
        save_checkpoint(firms, phase=2, path=cp_path)

    # ── Stage 2: Foursquare Discovery ──
    if start_stage <= 2 and not args.skip_foursquare:
        fsq_key = os.getenv("FOURSQUARE_API_KEY")
        if not fsq_key:
            print("[stage 2] WARNING: FOURSQUARE_API_KEY not found — skipping Foursquare")
        else:
            print("[stage 2] Foursquare API discovery...")
            fsq_firms = discover_foursquare(county_config, fsq_key, test_mode=args.test)
            before = len(firms)
            firms = _merge_foursquare(firms, fsq_firms)
            print(f"[stage 2] Merged: {before} + {len(fsq_firms)} Foursquare → {len(firms)} total\n")
        save_checkpoint(firms, phase=3, path=cp_path)

    # ── Intermediate dedup ──
    if start_stage <= 3:
        print("[dedup] Running intermediate deduplication...")
        log_path = os.path.join("data", "county", f"{slug}_potential_duplicates.log")
        firms = deduplicate_firms(firms, log_path=log_path)
        print(f"[dedup] {len(firms)} firms after intermediate dedup\n")
        save_checkpoint(firms, phase=4, path=cp_path)

    # ── Stage 3: Enhancement ──
    if start_stage <= 4 and not args.skip_enhance:
        firms = enhance_firms(firms, county_config, test_mode=args.test)
        save_checkpoint(firms, phase=5, path=cp_path)

    # ── Pre-dedup cleanup: normalize attorneys field ──
    for firm in firms:
        attorneys = firm.get("attorneys")
        if attorneys:
            normalized = []
            for a in attorneys:
                if isinstance(a, dict):
                    normalized.append(a.get("name", str(a)))
                else:
                    normalized.append(str(a))
            firm["attorneys"] = normalized

    # ── Final dedup ──
    print("\n[dedup] Running final deduplication...")
    log_path = os.path.join("data", "county", f"{slug}_final_duplicates.log")
    firms = deduplicate_firms(firms, log_path=log_path)
    print(f"[dedup] {len(firms)} firms after final dedup\n")

    # ── CSV output ──
    output_path = os.path.join(COUNTY_DATA_DIR, f"{slug}.csv")
    firm_count = firms_to_csv(firms, county_config, output_path)

    # ── Manifest update ──
    update_manifest(COUNTY_DATA_DIR)

    # ── Cleanup ──
    clear_checkpoint(cp_path)
    _print_summary(firms, county_config)

    print(f"\n  Output: {output_path}")
    print(f"  Firms: {firm_count}")


if __name__ == "__main__":
    main()
