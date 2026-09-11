#!/usr/bin/env python3
"""
Avvo discovery for a TX county — bulk list-page scrape (name, firm, phone,
address from JSON-LD "Person" blocks on /all-lawyers/tx/<city_slug>.html),
same technique as avvo_discover.py's KS version. Written to a raw cache,
NOT merged into the CSV directly — merge_avvo_data.py does the matching
(exact name+city, same conservative policy as Justia) and only fetches
each matched attorney's DETAIL page (which is the only place Avvo exposes
a firm website, via a JSON-LD LocalBusiness "sameAs" array) to backfill
website on existing no-website rows.

This split (cheap bulk list scrape now, expensive detail-page fetch only
for actual matches later) avoids the wasted cost of fetching a detail
page for lawyers we have no use for.

Usage: python3 avvo_tx_discover.py <slug> [city1] [city2] ...
"""
import json
import re
import sys
import time
from pathlib import Path

from curl_cffi import requests as creq
from bs4 import BeautifulSoup

CACHE_DIR = Path("data/county")

STATE_ABBR = "tx"

COUNTY_CITIES = {
    "harris-county-tx": [
        "Houston", "Pasadena", "Baytown", "Pearland", "Deer Park", "La Porte",
        "Humble", "Katy", "Spring", "Cypress", "Tomball", "Channelview",
        "South Houston", "Galena Park", "Jacinto City", "Bellaire",
        "West University Place", "Friendswood", "Webster", "Kingwood",
    ],
    "dallas-county-tx": [
        "Dallas", "Irving", "Garland", "Mesquite", "Grand Prairie",
        "Richardson", "Carrollton", "DeSoto", "Cedar Hill", "Duncanville",
        "Lancaster", "Farmers Branch", "Coppell", "Addison",
        "University Park", "Highland Park", "Balch Springs", "Rowlett",
        "Sachse", "Wylie", "Lewisville", "Grapevine",
    ],
    "tarrant-county-tx": [
        "Fort Worth", "Arlington", "North Richland Hills", "Mansfield",
        "Euless", "Bedford", "Hurst", "Haltom City", "Keller", "Southlake",
        "Colleyville", "Grapevine", "Watauga", "Saginaw", "Burleson",
        "Crowley", "Benbrook", "White Settlement", "Forest Hill",
        "Kennedale", "Everman", "Lake Worth",
    ],
    "bexar-county-tx": [
        "San Antonio", "Alamo Heights", "Castle Hills", "Converse",
        "Helotes", "Leon Valley", "Live Oak", "Olmos Park", "Schertz",
        "Shavano Park", "Terrell Hills", "Universal City", "Windcrest",
        "Fair Oaks Ranch",
    ],
}

CITY_SLUG_FIX = {
    "West University Place": "west_university_place",
    "St. Hedwig": "st_hedwig",
}


def city_slug(city: str) -> str:
    return CITY_SLUG_FIX.get(city, city.lower().replace(" ", "_").replace(".", ""))


def extract_attorneys_from_page(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    attorneys = []
    for block in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(block.string)
        except Exception:
            continue
        if data.get("@type") == "Person" and data.get("name"):
            works_for = data.get("worksFor") or {}
            addr = works_for.get("address") or {}
            attorneys.append({
                "name": data["name"],
                "firm": works_for.get("name") or data["name"],
                "phone": works_for.get("telephone") or "",
                "street": addr.get("streetAddress") or "",
                "city": addr.get("addressLocality") or "",
                "state": addr.get("addressRegion") or "",
                "zip": addr.get("postalCode") or "",
                "avvo_url": data.get("url") or data.get("@id") or "",
            })
    return attorneys


def scrape_city(session, city: str, delay: float = 0.8, max_pages: int = 150) -> list[dict]:
    # Validate against ONLY this specific city, not the whole county's city
    # set — Avvo's per-city listing page has a wide geographic radius, so
    # e.g. a small suburb's page keeps re-surfacing the county seat's own
    # attorneys. Validating against the full county list wrongly accepted
    # those as "new" results for every suburb, hitting the page cap on
    # every single small city instead of terminating quickly via the
    # empty-page rule below (found in the wild: Bexar County's tiny
    # Shavano Park/Universal City/Fair Oaks Ranch pages each burned all
    # 150 pages re-capturing San Antonio's own attorneys).
    target_city_lower = {city.lower().strip()}
    slug = city_slug(city)
    base_url = f"https://www.avvo.com/all-lawyers/{STATE_ABBR}/{slug}.html"
    results = []
    consecutive_empty = 0
    page = 1

    while page <= max_pages:
        url = base_url if page == 1 else f"{base_url}?page={page}"
        try:
            r = session.get(url, timeout=20)
            if r.status_code == 429:
                print(f"  {city} p{page}: rate limited, waiting 90s")
                time.sleep(90)
                r = session.get(url, timeout=20)
            if r.status_code != 200:
                print(f"  {city} p{page}: HTTP {r.status_code}, stopping")
                break
            attorneys = extract_attorneys_from_page(r.text)
            if not attorneys:
                break

            valid = [
                a for a in attorneys
                if a["city"].lower().strip() in target_city_lower
                and a["state"].upper() == "TX"
            ]
            results.extend(valid)

            if len(valid) == 0:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    break
            else:
                consecutive_empty = 0

            page += 1
            time.sleep(delay)
        except Exception as e:
            print(f"  {city} p{page}: error {e}, stopping")
            break

    print(f"  {city}: {page - 1} pages -> {len(results)} valid attorneys")
    return results


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 avvo_tx_discover.py <slug> [city1] [city2] ...")
        sys.exit(1)
    slug = sys.argv[1]
    cities = sys.argv[2:] if len(sys.argv) > 2 else COUNTY_CITIES.get(slug, [])
    if not cities:
        print(f"No city list known for {slug} — add one to COUNTY_CITIES or pass cities explicitly.")
        sys.exit(1)

    session = creq.Session(impersonate="chrome120")
    all_results = []
    for city in cities:
        print(f"=== {city} ===")
        all_results.extend(scrape_city(session, city))

    out_path = CACHE_DIR / f"{slug}_avvo_cache.json"
    existing = []
    if out_path.exists():
        existing = json.loads(out_path.read_text())
    existing.extend(all_results)
    out_path.write_text(json.dumps(existing, indent=1))
    print(f"\nTotal this run: {len(all_results)}. Cache total: {len(existing)}")


if __name__ == "__main__":
    main()
