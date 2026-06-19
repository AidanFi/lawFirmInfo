#!/usr/bin/env python3
"""Deep clean for Miami & Linn County KS CSVs.

Removes confirmed non-law businesses, government offices, out-of-county firms,
and web-search garbage; merges duplicate firm records (filling empty fields);
clears dead/mismatched website URLs; recomputes priority from practice_area.

Per the include-all-firms policy, entries are NEVER removed for missing
phone/website/email — only for being non-law / out-of-county / not real.

Usage: python3 deep_clean_miami_linn.py
"""
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from scraper.county.config import get_priority

DATA_DIR = Path("app/county-data")

# ---------------------------------------------------------------------------
# Per-county curation lists (exact firm names as they appear in the CSV)
# ---------------------------------------------------------------------------

CONFIG = {
    "miami-county-ks": {
        # Confirmed non-law, government, out-of-county, or web-search garbage.
        "remove": {
            "Vance C. Preman PC",                                   # KCMO firm (4330 Belleview, KC MO 64111)
            "Highway Seven Auto Parts",                             # auto parts
            "Cy & Dee's Blackberry Patch",                          # farm/food
            "Heartland Print & Design formerly Partners Print & Copy",  # print shop
            "The Henry Law Firm, P.A.",                             # Overland Park (Johnson County) per firm site
            "Vintage Park At Louisburg",                            # assisted living
            "Cow Palace",                                           # restaurant/venue
            "Oil Patch Pump & Supply",                              # oilfield supply
            "B&P Services LLC",                                     # generic services, not a law firm
            "District Court Judge 6th Judicial District",           # government court
            "Family Center Farm & Home of Paola",                  # farm/home store
            "Kansas Wildlife, Parks and Tourism - Hillsdale Wildlife Area",  # government agency
            "LS Contracting LLC",                                   # contractor
            "Legal Animation - DC Area Moms",                       # web-search garbage
            "Man Requests 'Trial By Combat' To Settle Legal Dispute With Ex-Wife",  # news article
            "Reviews - Guadagno Law, PLLC",                         # out-of-area (Seattle) scrape artifact
            "S&S Stables, LLC",                                     # horse stables
            "Splashtacular, LLC",                                   # water park
        },
        # canonical_name -> [duplicate names to fold in and drop]
        "merge": {
            "Law Office of Lee H. Tetwiler": ["Law Offices of Lee H. Tetwiler"],
            "Barkis Law Office": ["Law Offices of Marvin Barkis, Attorney, 112 S Pearl St"],
            "Law Office of Sheila M. Schultz": ["Sheila Schultz Law Office"],
            "Mary Stephenson, Attorney at Law": ["Stephenson Mary Atty At Law"],
            "Steven A. Jensen": ["Jensen Steven A"],
            "Nicholson Law Office LC": ["Nicholson Robert I Attorney"],
            "Kimberly D. Burris, Attorney at Law": ["Burris Law Office"],
        },
        # names whose website should be blanked (dead / parked / wrong firm)
        "clear_website": {
            "5th Generation Legal Advisors",        # DuckDuckGo redirect, no real site
            "Amy C. Winterscheid Attorney at Law",  # NXDOMAIN
            "Barkis Law Office",                    # unreachable
            "Glen E. Sharp, II",                    # NXDOMAIN
            "Steven A. Jensen",                     # NXDOMAIN
            "Jensen Steven A",                      # NXDOMAIN (dup, merged)
            "Nicholson Law Office LC",              # pointed at hartleylawgroupllc.com (wrong firm)
        },
        # names -> corrected website URL
        "fix_website": {
            "Domoney & Domoney": "https://www.domoneylaw.com",
        },
    },
    "linn-county-ks": {
        "remove": {
            "NAPA Auto Parts",                                       # auto parts
            "John and Karla's Camping Palace",                       # campground
            "6th Judicial District - Linn County",                   # government court
            "African American Lawyers/Attorneys, Kentucky, 1880-1940",  # UK archive garbage
            "Attorneys - Schindler Law Firm",                        # St. Louis MO firm (314)
            "Big Ideas Gun & Pawn",                                  # gun/pawn shop
            "Contact - Law Office of Michelle Poblenz",              # Texas firm (469)
            "Forbach Aromatics LLC",                                 # aromatics
            "Greensfelder partner Chris Pickett specializes in employment law",  # STL MO news article
            "Israel S. Nelson - Brinker & Doyen, LLP",               # St. Louis MO firm (314)
            "Linn County Attorney",                                  # government prosecutor office
            "Louie & Sons Excavating LLC.",                          # excavating
            "Poole Paint & Body",                                    # auto body
            "Risk Management Partners",                              # insurance/risk, not law
            "Flying Monkeys Landing Pad",                            # bar/venue
            "Gainer Cattle Pasture",                                 # agriculture
            "Big Smokes BBQ/Pie Pantry",                             # restaurant
            "Carsten Ag Service LLC",                                # agriculture
            "Stone Excavating LLC",                                  # excavating
        },
        "merge": {
            # Two identical "Hodgson Legal Resources" rows (La Cygne + Parker)
            "Hodgson Legal Resources": ["Hodgson Legal Resources"],
        },
        "clear_website": {
            "Harding Law Firm, LLC",      # .org 404, .com unreachable
            "Hodgson Legal Resources",    # unreachable
        },
        "fix_website": {},
    },
}

FILL_FIELDS = ["website", "google_business_profile", "legal_directory_listing",
               "phone_number", "email", "practice_area", "street_address",
               "zip_code", "number_of_lawyers"]


def clean_county(slug: str, cfg: dict):
    path = DATA_DIR / f"{slug}.csv"
    rows = list(csv.DictReader(path.open()))
    fieldnames = list(rows[0].keys())
    before = len(rows)

    # 1. Website fixes/clears (apply by name to ALL matching rows)
    for r in rows:
        name = r["law_firm_name"]
        if name in cfg["clear_website"]:
            r["website"] = ""
        if name in cfg["fix_website"]:
            r["website"] = cfg["fix_website"][name]

    # 2. Merge duplicates — fill canonical's empty fields from dups, drop dups.
    by_name = {}
    for r in rows:
        by_name.setdefault(r["law_firm_name"], []).append(r)

    dropped_for_merge = set()
    merged_count = 0
    for canonical, dups in cfg["merge"].items():
        canon_rows = by_name.get(canonical, [])
        if not canon_rows:
            continue
        keep = canon_rows[0]
        # Extra rows with the canonical name itself (self-duplicates) also fold in.
        donors = canon_rows[1:]
        for dname in dups:
            if dname == canonical:
                continue
            donors.extend(by_name.get(dname, []))
        for donor in donors:
            for f in FILL_FIELDS:
                if not keep.get(f, "").strip() and donor.get(f, "").strip():
                    keep[f] = donor[f]
            dropped_for_merge.add(id(donor))
            merged_count += 1

    # 3. Build output: drop removed + merged-away rows
    removed = []
    kept = []
    for r in rows:
        if id(r) in dropped_for_merge:
            continue
        if r["law_firm_name"] in cfg["remove"]:
            removed.append(r["law_firm_name"])
            continue
        kept.append(r)

    # 4. Recompute priority from practice_area
    for r in kept:
        pa = r.get("practice_area", "").strip() or "General"
        r["priority"] = str(get_priority(pa))

    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(kept)

    print(f"\n=== {slug} ===")
    print(f"  Before:        {before}")
    print(f"  Removed:       {len(removed)} (non-law/govt/out-of-county/garbage)")
    print(f"  Merged away:   {merged_count} duplicate rows")
    print(f"  After:         {len(kept)}")
    print(f"  Removed names:")
    for n in sorted(removed):
        print(f"    - {n}")


if __name__ == "__main__":
    for slug, cfg in CONFIG.items():
        clean_county(slug, cfg)
