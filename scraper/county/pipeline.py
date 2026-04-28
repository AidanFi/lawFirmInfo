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
import json
import os
import re
import uuid
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
STATEWIDE_DATA_PATH = os.path.join("app", "firms_data.js")


def _import_statewide(firms: list, county_config: dict) -> list:
    if not os.path.exists(STATEWIDE_DATA_PATH):
        print("[statewide] firms_data.js not found — skipping")
        return firms

    with open(STATEWIDE_DATA_PATH) as f:
        text = f.read()

    json_str = re.sub(r'^const FIRMS_DATA = ', '', text).rstrip().rstrip(';')
    data = json.loads(json_str)

    county_cities_lower = {c.lower() for c in county_config["cities"]}
    county_state = county_config["state"]
    county_zips = set(county_config.get("zip_codes", []))

    candidates = []
    for sf in data["firms"]:
        addr = sf.get("address") or {}
        city = addr.get("city", "").lower()
        state = addr.get("state", "")
        zip_code = addr.get("zip", "")

        in_county = (
            (city in county_cities_lower and state == county_state)
            or (county_zips and zip_code in county_zips)
        )
        if not in_county:
            continue
        candidates.append(sf)

    added = 0
    enriched = 0
    for sf in candidates:
        sf_addr = sf.get("address") or {}
        sf_city = sf_addr.get("city", "")

        matched = None
        for firm in firms:
            firm_city = (firm.get("address") or {}).get("city", "")
            if firm_city.lower() == sf_city.lower() and are_same_firm(sf["name"], firm["name"]):
                matched = firm
                break

        if matched:
            if not matched.get("phone") and sf.get("phone"):
                matched["phone"] = sf["phone"]
            if not matched.get("website") and sf.get("website"):
                matched["website"] = sf["website"]
            if not matched.get("email") and sf.get("email"):
                matched["email"] = sf["email"]
            m_addr = matched.get("address") or {}
            if not m_addr.get("street") and sf_addr.get("street"):
                m_addr["street"] = sf_addr["street"]
            if not m_addr.get("zip") and sf_addr.get("zip"):
                m_addr["zip"] = sf_addr["zip"]
            for src in sf.get("sources", []):
                sources = matched.setdefault("sources", [])
                if src not in sources:
                    sources.append(src)
            enriched += 1
        else:
            if not sf.get("phone") and not sf.get("website") and not sf.get("email"):
                continue
            firms.append({
                "id": str(uuid.uuid4()),
                "name": sf["name"],
                "practiceAreas": sf.get("practiceAreas", []),
                "summary": sf.get("summary"),
                "website": sf.get("website"),
                "phone": sf.get("phone"),
                "email": sf.get("email"),
                "address": sf_addr,
                "coordinates": sf.get("coordinates"),
                "sources": sf.get("sources", []),
                "google_business_profile": "",
            })
            added += 1

    print(f"  [statewide] Scanned {len(candidates)} matching entries")
    print(f"  [statewide] Enriched {enriched} existing, added {added} new firms")
    return firms


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


def _pct(n, total):
    return f"{100 * n / total:.0f}%" if total else "0%"


def _print_summary(firms: list, county_config: dict):
    source_counts = Counter()
    for f in firms:
        for s in f.get("sources", []):
            source_counts[s] += 1

    n = len(firms)
    has_website = sum(1 for f in firms if f.get("website"))
    has_gbp = sum(1 for f in firms if f.get("google_business_profile"))
    has_directory = sum(1 for f in firms if
                       f.get("martindale_url") or f.get("justia_url") or
                       f.get("avvo_url") or f.get("findlaw_url"))
    has_phone = sum(1 for f in firms if f.get("phone"))
    has_email = sum(1 for f in firms if f.get("email"))
    has_practice = sum(1 for f in firms if f.get("practiceAreas"))
    has_any_link = sum(
        1 for f in firms
        if f.get("website") or f.get("google_business_profile") or
        f.get("martindale_url") or f.get("justia_url") or
        f.get("avvo_url") or f.get("findlaw_url")
    )
    has_2plus = sum(
        1 for f in firms
        if sum(bool(f.get(k)) for k in ("website", "phone", "email")) >= 2
    )

    print(f"\n{'=' * 55}")
    print(f"  County Scrape Complete: {county_config['name']}, {county_config['state']}")
    print(f"{'=' * 55}")
    print(f"  Total firms: {n}")
    print(f"\n  Data sources:")
    for src, count in source_counts.most_common():
        print(f"    {src}: {count}")
    print(f"\n  Coverage:")
    print(f"    Website:           {has_website}/{n} ({_pct(has_website, n)})")
    print(f"    Phone:             {has_phone}/{n} ({_pct(has_phone, n)})")
    print(f"    Email:             {has_email}/{n} ({_pct(has_email, n)})")
    print(f"    Google profile:    {has_gbp}/{n} ({_pct(has_gbp, n)})")
    print(f"    Directory listing: {has_directory}/{n} ({_pct(has_directory, n)})")
    print(f"    Any link/URL:      {has_any_link}/{n} ({_pct(has_any_link, n)})")
    print(f"    Practice area:     {has_practice}/{n} ({_pct(has_practice, n)})")
    print(f"\n  Quality:")
    print(f"    2+ contact methods: {has_2plus}/{n} ({_pct(has_2plus, n)})")
    print(f"{'=' * 55}")


def main():
    parser = argparse.ArgumentParser(description="County-Level Law Firm Data Pipeline")
    parser.add_argument("--county", required=True,
                        help="County key (e.g., 'johnson')")
    parser.add_argument("--skip-foursquare", action="store_true",
                        help="Skip Foursquare discovery phase")
    parser.add_argument("--skip-enhance", action="store_true",
                        help="Skip enhancement pass")
    parser.add_argument("--skip-ks-courts", action="store_true",
                        help="Skip KS Courts scraper in enhancement")
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

    # ── Stage 2b: Import from statewide data ──
    if start_stage <= 3:
        print("[stage 2b] Importing from statewide data...")
        before = len(firms)
        firms = _import_statewide(firms, county_config)
        print(f"[stage 2b] {before} → {len(firms)} firms after statewide import\n")

    # ── Intermediate dedup ──
    if start_stage <= 3:
        print("[dedup] Running intermediate deduplication...")
        log_path = os.path.join("data", "county", f"{slug}_potential_duplicates.log")
        firms = deduplicate_firms(firms, log_path=log_path)
        print(f"[dedup] {len(firms)} firms after intermediate dedup\n")
        save_checkpoint(firms, phase=4, path=cp_path)

    # ── Stage 3: Enhancement ──
    if start_stage <= 4 and not args.skip_enhance:
        firms = enhance_firms(
            firms, county_config,
            test_mode=args.test, skip_ks_courts=args.skip_ks_courts,
        )
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
