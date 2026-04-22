import time
import uuid

import requests

from scraper.utils.normalize import are_same_firm
from scraper.county.config import SEARCH_QUERIES, FOURSQUARE_LEGAL_CATEGORIES

API_BASE = "https://places-api.foursquare.com/places/search"

_LEGAL_CATEGORY_IDS = {
    "52f2ab2ebcbc57f1066b8b3f",  # Law Office
    "63be6904847c3692a84b9b6b",  # Legal Service
}

_LAW_KEYWORDS = (
    "law", "legal", "attorney", "lawyer", "counsel", "firm",
    "llc", "llp", "pllc", "p.a.", " pa", " pc", "chartered",
    "esquire", "esq", "advocates",
)


def _is_likely_law_firm(place: dict) -> bool:
    categories = place.get("categories", [])
    cat_ids = {c.get("fsq_category_id", "") for c in categories}
    if cat_ids & _LEGAL_CATEGORY_IDS:
        return True
    name_lower = place.get("name", "").lower()
    return any(kw in name_lower for kw in _LAW_KEYWORDS)


def _is_duplicate(name: str, city: str, existing: list) -> bool:
    for firm in existing:
        if firm["address"]["city"].lower() == city.lower():
            if are_same_firm(name, firm["name"]):
                return True
    return False


def _parse_location(place: dict) -> dict:
    loc = place.get("location", {})
    return {
        "street": loc.get("address", ""),
        "city": loc.get("locality", ""),
        "county": "",
        "state": loc.get("region", ""),
        "zip": loc.get("postcode", ""),
    }


def discover_foursquare(county_config: dict, api_key: str, test_mode: bool = False) -> list:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "X-Places-Api-Version": "2025-06-17",
    }

    firms = []
    state = county_config["state"]
    cities = county_config["cities"]
    county_cities_lower = {c.lower() for c in county_config["cities"]}
    if test_mode:
        cities = cities[:3]

    api_calls = 0
    skipped_out_of_county = 0
    skipped_not_legal = 0

    for city in cities:
        for query in SEARCH_QUERIES:
            params = {
                "query": query,
                "near": f"{city}, {state}",
                "categories": FOURSQUARE_LEGAL_CATEGORIES,
                "limit": 50,
            }

            try:
                resp = requests.get(API_BASE, headers=headers, params=params, timeout=15)
                api_calls += 1
                resp.raise_for_status()
                data = resp.json()
            except requests.RequestException as e:
                print(f"  [foursquare] Error searching '{query}' in {city}: {e}")
                continue

            results = data.get("results", [])
            for place in results:
                if not _is_likely_law_firm(place):
                    skipped_not_legal += 1
                    continue
                name = place.get("name", "")
                address = _parse_location(place)
                city_val = address["city"] or city

                if city_val.lower() not in county_cities_lower:
                    skipped_out_of_county += 1
                    continue

                if _is_duplicate(name, city_val, firms):
                    continue

                geo = place.get("geocodes", {}).get("main", {})
                coords = None
                if geo.get("latitude") and geo.get("longitude"):
                    coords = {"lat": geo["latitude"], "lng": geo["longitude"]}

                firms.append({
                    "id": str(uuid.uuid4()),
                    "name": name,
                    "practiceAreas": [],
                    "summary": None,
                    "website": place.get("website"),
                    "phone": place.get("tel"),
                    "email": None,
                    "address": address,
                    "coordinates": coords,
                    "sources": ["foursquare"],
                    "google_business_profile": "",
                })

            time.sleep(0.5)

            if test_mode and len(firms) >= 10:
                break
        if test_mode and len(firms) >= 10:
            break

    print(f"  [foursquare] API calls: {api_calls}")
    if skipped_not_legal:
        print(f"  [foursquare] Skipped {skipped_not_legal} non-legal results")
    if skipped_out_of_county:
        print(f"  [foursquare] Skipped {skipped_out_of_county} out-of-county results")
    print(f"  [foursquare] Discovered {len(firms)} firms")
    return firms
