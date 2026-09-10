#!/usr/bin/env python3
"""
Justia lawyer-directory discovery for Harris County, TX cities.

Supplementary website-backfill source: Justia listing cards give name,
phone, address, and (when the attorney has one) a real website link. TX
listing pages don't expose a reliable "true last page" the way the KS
pages did — the pagination widget just keeps serving content, eventually
cycling back to repeats of the same attorneys reordered — so instead of
trusting a max-page number, this stops a city once a full page yields
zero previously-unseen attorney names.

This is a DISCOVERY cache only — matching it back to CSV rows for
website backfill is a separate, conservative step (see
merge_justia_data.py) that requires close name+city agreement, never
bare fuzzy-name similarity (verified elsewhere in this project that
fuzzy matching on personal names causes false merges).
"""
import json
import re
import sys
import time
from pathlib import Path

from curl_cffi import requests as creq
from bs4 import BeautifulSoup

CACHE_DIR = Path("data/county")

HARRIS_CITIES = [
    "Houston", "Pasadena", "Baytown", "Pearland", "Deer Park", "La Porte",
    "Humble", "Katy", "Spring", "Cypress", "Tomball", "Channelview",
    "South Houston", "Galena Park", "Jacinto City", "Bellaire",
    "West University Place", "Friendswood", "Webster", "Kingwood",
]
CITY_SLUGS = {
    "West University Place": "west-university-place",
}


def parse_address(addr_el):
    if not addr_el:
        return "", "", "", ""
    text = addr_el.get_text(separator="|", strip=True)
    parts = [p.strip() for p in text.split("|") if p.strip()]
    street, city, state, zipcode = "", "", "", ""
    if parts:
        loc = parts[-1]
        m = re.match(r"^(.+?),?\s*([A-Z]{2})\s*(\d{5})?$", loc)
        if m:
            city = m.group(1).strip()
            state = m.group(2).strip()
            zipcode = (m.group(3) or "").strip()
        street = " ".join(parts[:-1])
    return street, city, state, zipcode


def extract_cards(soup, target_cities_lower: set[str]) -> list[dict]:
    results = []
    for card in soup.find_all("div", class_=re.compile(r"jld-card")):
        name_el = card.find("strong", class_="name")
        if not name_el:
            continue
        name_link = name_el.find("a")
        name = name_link.get_text(strip=True) if name_link else name_el.get_text(strip=True)
        if not name:
            continue

        addr_el = card.find("div", class_=re.compile("address"))
        street, city, state, zipcode = parse_address(addr_el)
        if not city or city.lower() not in target_cities_lower:
            continue
        if state and state.upper() != "TX":
            continue

        phone = ""
        phone_el = card.find("strong", class_=re.compile("phone"))
        if phone_el:
            phone_link = phone_el.find("a", href=re.compile(r"^tel:"))
            if phone_link:
                phone = phone_link.get_text(strip=True)

        website = ""
        for link in card.find_all("a", href=True):
            if link.get("data-button-tag") == "website":
                href = link.get("href", "")
                if href.startswith("http") and "justia.com" not in href:
                    website = href
                break

        outline = card.find("div", class_=re.compile("outline"))
        practice_line = outline.get_text(strip=True) if outline else ""

        results.append({
            "name": name, "phone": phone, "website": website,
            "street": street, "city": city, "state": state, "zip": zipcode,
            "practice_line": practice_line,
        })
    return results


def scrape_city(session, city: str, max_pages: int = 200, delay: float = 1.0) -> list[dict]:
    slug = CITY_SLUGS.get(city, city.lower().replace(" ", "-"))
    base_url = f"https://www.justia.com/lawyers/texas/{slug}"
    target = {city.lower()}
    seen_names = set()
    results = []
    empty_streak = 0

    for page in range(1, max_pages + 1):
        url = base_url if page == 1 else f"{base_url}?page={page}"
        try:
            r = session.get(url, timeout=20)
        except Exception as e:
            print(f"  {city} p{page}: error {e}, stopping")
            break
        if r.status_code == 429:
            print(f"  {city} p{page}: rate limited, waiting 30s")
            time.sleep(30)
            continue
        if r.status_code != 200:
            print(f"  {city} p{page}: HTTP {r.status_code}, stopping")
            break

        soup = BeautifulSoup(r.text, "lxml")
        cards = extract_cards(soup, target)
        if not cards:
            break

        new_count = 0
        for c in cards:
            key = c["name"].lower().strip()
            if key not in seen_names:
                seen_names.add(key)
                results.append(c)
                new_count += 1

        if new_count == 0:
            empty_streak += 1
            if empty_streak >= 2:
                break
        else:
            empty_streak = 0

        time.sleep(delay)

    print(f"  {city}: {len(results)} unique attorneys found")
    return results


def main():
    session = creq.Session(impersonate="chrome120")
    all_results = []
    cities = sys.argv[1:] if len(sys.argv) > 1 else HARRIS_CITIES
    for city in cities:
        print(f"=== {city} ===")
        all_results.extend(scrape_city(session, city))

    out_path = CACHE_DIR / "harris-county-tx_justia_cache.json"
    existing = []
    if out_path.exists():
        existing = json.loads(out_path.read_text())
    existing.extend(all_results)
    out_path.write_text(json.dumps(existing, indent=1))
    print(f"\nTotal this run: {len(all_results)}. Cache total: {len(existing)}")


if __name__ == "__main__":
    main()
