"""Yelp Fusion API discovery for county law firms.

Uses the Business Search endpoint (free tier, 500 calls/day).
Returns name, phone, address, and coordinates — no website (not in Yelp's
search response). Websites are populated later by the enhancement phase.
"""
import time
import uuid

import requests

from scraper.utils.normalize import are_same_firm

SEARCH_URL = "https://api.yelp.com/v3/businesses/search"

_LAW_KEYWORDS = (
    "law", "legal", "attorney", "lawyer", "counsel", "firm",
    "llc", "llp", "pllc", "p.a.", " pa", " pc", "chartered",
)


def _is_likely_law(name: str, categories: list) -> bool:
    aliases = {c.get("alias", "") for c in categories}
    if "lawyers" in aliases or "legalservices" in aliases:
        return True
    return any(kw in name.lower() for kw in _LAW_KEYWORDS)


def _is_duplicate(name: str, city: str, existing: list) -> bool:
    for firm in existing:
        if firm["address"]["city"].lower() == city.lower():
            if are_same_firm(name, firm["name"]):
                return True
    return False


def discover_yelp(county_config: dict, api_key: str, test_mode: bool = False) -> list:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }

    firms = []
    state = county_config["state"]
    cities = county_config["cities"][:]
    county_cities_lower = {c.lower() for c in county_config["cities"]}
    zip_codes = set(county_config.get("zip_codes", []))

    if test_mode:
        cities = cities[:2]

    search_calls = 0
    skipped = 0

    for city in cities:
        location = f"{city}, {state}"
        offset = 0

        while True:
            params = {
                "categories": "lawyers",
                "location": location,
                "limit": 50,
                "offset": offset,
            }
            try:
                resp = requests.get(SEARCH_URL, headers=headers, params=params, timeout=15)
                search_calls += 1
                resp.raise_for_status()
                data = resp.json()
            except requests.RequestException as e:
                print(f"  [yelp] Error searching {location}: {e}")
                break

            businesses = data.get("businesses", [])
            if not businesses:
                break

            for biz in businesses:
                name = biz.get("name", "")
                cats = biz.get("categories", [])

                if not _is_likely_law(name, cats):
                    skipped += 1
                    continue

                loc = biz.get("location", {})
                city_val = loc.get("city", city)
                biz_zip = loc.get("zip_code", "")

                in_county = (
                    city_val.lower() in county_cities_lower
                    or (zip_codes and biz_zip in zip_codes)
                )
                if not in_county:
                    skipped += 1
                    continue

                if _is_duplicate(name, city_val, firms):
                    continue

                geo = biz.get("coordinates", {})
                coords = None
                if geo.get("latitude") and geo.get("longitude"):
                    coords = {"lat": geo["latitude"], "lng": geo["longitude"]}

                phone = biz.get("display_phone") or biz.get("phone") or ""

                firms.append({
                    "id": str(uuid.uuid4()),
                    "name": name,
                    "practiceAreas": [],
                    "summary": None,
                    "website": None,
                    "phone": phone,
                    "email": None,
                    "address": {
                        "street": loc.get("address1", ""),
                        "city": city_val,
                        "county": "",
                        "state": loc.get("state", state),
                        "zip": biz_zip,
                    },
                    "coordinates": coords,
                    "sources": ["yelp"],
                    "google_business_profile": "",
                })

                time.sleep(0.1)

                if test_mode and len(firms) >= 15:
                    break

            total = data.get("total", 0)
            offset += len(businesses)
            if offset >= min(total, 240) or test_mode:
                break
            time.sleep(0.5)

        if test_mode and len(firms) >= 15:
            break

    print(f"  [yelp] API calls: {search_calls}")
    if skipped:
        print(f"  [yelp] Skipped {skipped} non-law/out-of-county")
    print(f"  [yelp] Discovered {len(firms)} firms")
    return firms
