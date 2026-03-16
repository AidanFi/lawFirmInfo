#!/usr/bin/env python3
"""
Kansas Law Firm Directory Scraper

Usage:
  python scraper/scraper.py          # Full run (~30-60 min)
  python scraper/scraper.py --test   # Quick test: 5 cities, no website scraping
"""
import argparse, os, time
import googlemaps
from dotenv import load_dotenv

from scraper.phases.google_places import scrape_google_places, KANSAS_CITIES
from scraper.phases.ksbar import scrape_ksbar, merge_ksbar_into_firms
from scraper.phases.website_scraper import scrape_firm_website
from scraper.utils.normalize import normalize_practice_area
from scraper.utils.referral import calculate_referral_score
from scraper.utils.checkpoint import save_checkpoint, load_checkpoint, clear_checkpoint
from scraper.utils.output import write_firms_data_js

TEST_CITIES = ["Wichita", "Topeka", "Kansas City", "Overland Park", "Lawrence"]
CHECKPOINT_PATH = "data/checkpoint.json"
OUTPUT_PATH = "app/firms_data.js"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Quick test run (5 cities, no website scraping)")
    args = parser.parse_args()

    load_dotenv("scraper/.env")
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        print("ERROR: GOOGLE_MAPS_API_KEY not set in scraper/.env")
        return

    cities = TEST_CITIES if args.test else KANSAS_CITIES

    # Resume from checkpoint if available
    checkpoint = load_checkpoint(CHECKPOINT_PATH)
    if checkpoint:
        print(f"[checkpoint] Resuming from phase {checkpoint['phase']} with {len(checkpoint['firms'])} firms")
        firms = checkpoint["firms"]
        start_phase = checkpoint["phase"]
    else:
        firms = []
        start_phase = 1

    client = googlemaps.Client(key=api_key)

    # Phase 1: Google Places
    if start_phase <= 1:
        print(f"[phase 1] Searching Google Places for {len(cities)} cities...")
        firms = scrape_google_places(client, cities=cities)
        print(f"[phase 1] Found {len(firms)} firms")
        save_checkpoint(firms, phase=2, path=CHECKPOINT_PATH)

    # Phase 2: KSBar
    if start_phase <= 2:
        print("[phase 2] Scraping Kansas Bar Association directory...")
        ksbar_entries = scrape_ksbar()
        firms = merge_ksbar_into_firms(firms, ksbar_entries)
        print(f"[phase 2] Total after KSBar merge: {len(firms)} firms")
        save_checkpoint(firms, phase=3, path=CHECKPOINT_PATH)

    # Phase 3: Website scraping (skip in test mode)
    if start_phase <= 3 and not args.test:
        print("[phase 3] Scraping firm websites...")
        for i, firm in enumerate(firms):
            if not firm.get("website"):
                continue
            result = scrape_firm_website(firm["website"], firm["name"], firm["address"]["city"])
            firm["summary"] = result["summary"]
            if result["email"]:
                firm["email"] = result["email"]
            if (i + 1) % 50 == 0:
                save_checkpoint(firms, phase=3, path=CHECKPOINT_PATH)
                print(f"[phase 3] Progress: {i + 1}/{len(firms)}")
            time.sleep(1)

    # Phase 4: Finalize
    print("[phase 4] Calculating referral scores and writing output...")
    for firm in firms:
        firm["practiceAreas"] = [normalize_practice_area(p) for p in firm.get("practiceAreas", [])]
        firm["referralScore"] = calculate_referral_score(firm["practiceAreas"])
        if not firm.get("summary"):
            firm["summary"] = f"{firm['name']} — law firm in {firm['address']['city']}, Kansas"

    write_firms_data_js(firms, path=OUTPUT_PATH)
    clear_checkpoint(CHECKPOINT_PATH)

    # Summary
    from collections import Counter
    scores = Counter(f["referralScore"] for f in firms)
    missing_areas = sum(1 for f in firms if not f["practiceAreas"])
    missing_coords = sum(1 for f in firms if not f["coordinates"])
    print(f"\n=== Scrape Complete ===")
    print(f"Total firms: {len(firms)}")
    print(f"  Competitor: {scores['competitor']} | High: {scores['high']} | Medium: {scores['medium']} | Low: {scores['low']}")
    print(f"  Missing practice areas: {missing_areas}")
    print(f"  Missing coordinates: {missing_coords}")
    print(f"Output written to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
