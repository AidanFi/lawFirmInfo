"""API discovery of insurance agents/agencies in Johnson County, KS.

Queries the Yelp Fusion API and the Foursquare Places API for businesses
tagged as insurance-related (broad category — life/health agents included;
auto-insurance filtering happens in a later pipeline step) across every
city in Johnson County, KS.

Writes combined raw results to output/api_agents_raw.json as a JSON array
of: {name, phone, street_address, city, zip, website, source_url, source}.

Only real API responses are written — no fabricated/guessed data. If a
source's API key is missing or rejected, that source is skipped and the
failure is reported at the end; the other source's results are still
written.
"""
import json
import os
import re
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent.parent
OUTPUT_DIR = HERE / "output"
OUTPUT_PATH = OUTPUT_DIR / "api_agents_raw.json"

STATE = "KS"
CITIES = [
    "Overland Park", "Olathe", "Shawnee", "Lenexa", "Leawood",
    "Prairie Village", "Merriam", "Mission", "Gardner", "Spring Hill",
    "De Soto", "Edgerton", "Roeland Park", "Fairway", "Westwood",
    "Lake Quivira", "Mission Hills", "Mission Woods", "Westwood Hills",
]

YELP_SEARCH_URL = "https://api.yelp.com/v3/businesses/search"
YELP_PAGE_LIMIT = 50
YELP_MAX_OFFSET = 1000  # Yelp Fusion hard cap: offset + limit <= 1000

FOURSQUARE_SEARCH_URL = "https://places-api.foursquare.com/places/search"
FOURSQUARE_API_VERSION = "2025-06-17"
# "Insurance Agency" in Foursquare's current category taxonomy, confirmed
# live against the /places/search endpoint on 2026-08-22 (a result for a
# Farmers Insurance agent in Overland Park, KS carried this category id).
FOURSQUARE_INSURANCE_CATEGORY = "58daa1558bbb0b01f18ec1f1"
FOURSQUARE_PAGE_LIMIT = 50
FOURSQUARE_MAX_PAGES = 20  # safety cap; mirrors Yelp's ~1000-result ceiling

# "Shawnee Mission" is the USPS mailing-city alias shared by several small
# Johnson County cities (Mission, Prairie Village, Fairway, Westwood, Mission
# Hills, Mission Woods, Roeland Park all have addresses that geocode/post as
# "Shawnee Mission, KS"). Treat it as in-county for the locality sanity check.
_IN_COUNTY_LOCALITY_ALIASES = {"shawnee mission", "mission township"}


def load_api_keys() -> dict:
    """Load keys from scraper/.env first, falling back to repo-root .env."""
    load_dotenv(REPO_ROOT / "scraper" / ".env")
    load_dotenv(REPO_ROOT / ".env", override=False)
    return {
        "yelp": os.environ.get("YELP_API_KEY", "").strip(),
        "foursquare": os.environ.get("FOURSQUARE_API_KEY", "").strip(),
    }


def _digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def _dedupe(records: list) -> list:
    seen = set()
    out = []
    for r in records:
        key = (r["name"].strip().lower(), _digits(r["phone"]))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


# ---------------------------------------------------------------------------
# Yelp Fusion
# ---------------------------------------------------------------------------

def discover_yelp(api_key: str, errors: list) -> list:
    if not api_key:
        errors.append("Yelp: no API key found (YELP_API_KEY not set)")
        return []

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }
    records = []
    auth_failed = False

    for city in CITIES:
        if auth_failed:
            break
        location = f"{city}, {STATE}"
        offset = 0
        while True:
            params = {
                "categories": "insurance",
                "location": location,
                "limit": YELP_PAGE_LIMIT,
                "offset": offset,
            }
            try:
                resp = requests.get(YELP_SEARCH_URL, headers=headers, params=params, timeout=15)
            except requests.RequestException as e:
                errors.append(f"Yelp: request error for {location}: {e}")
                break

            if resp.status_code == 401:
                errors.append(
                    f"Yelp: 401 Unauthorized ({resp.text[:200]}) — API key is "
                    "invalid/expired/revoked. Aborting Yelp discovery."
                )
                auth_failed = True
                break
            if resp.status_code == 429:
                errors.append(f"Yelp: 429 rate-limited at {location} offset {offset}. Stopping Yelp discovery.")
                auth_failed = True
                break
            try:
                resp.raise_for_status()
            except requests.RequestException as e:
                errors.append(f"Yelp: HTTP error for {location}: {e} — {resp.text[:200]}")
                break

            data = resp.json()
            businesses = data.get("businesses", [])
            if not businesses:
                break

            for biz in businesses:
                loc = biz.get("location", {})
                street = ", ".join(
                    p for p in [loc.get("address1", ""), loc.get("address2", "")] if p
                )
                records.append({
                    "name": biz.get("name", "") or "",
                    "phone": biz.get("display_phone") or biz.get("phone") or "",
                    "street_address": street,
                    "city": loc.get("city", "") or city,
                    "zip": loc.get("zip_code", "") or "",
                    "website": None,  # Yelp business-search response has no website field
                    "source_url": (biz.get("url") or "").split("?")[0],
                    "source": "Yelp",
                })

            total = data.get("total", 0)
            offset += len(businesses)
            if offset >= min(total, YELP_MAX_OFFSET) or len(businesses) < YELP_PAGE_LIMIT:
                break
            time.sleep(0.3)

        time.sleep(0.2)

    return records


# ---------------------------------------------------------------------------
# Foursquare Places
# ---------------------------------------------------------------------------

def discover_foursquare(api_key: str, errors: list, cities=None, state=None,
                         county_qualifier="Johnson County", locality_aliases=None) -> list:
    if not api_key:
        errors.append("Foursquare: no API key found (FOURSQUARE_API_KEY not set)")
        return []

    cities = cities or CITIES
    state = state or STATE
    locality_aliases = locality_aliases if locality_aliases is not None else _IN_COUNTY_LOCALITY_ALIASES

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "X-Places-Api-Version": FOURSQUARE_API_VERSION,
    }
    city_lookup = {c.lower() for c in cities} | locality_aliases
    records = []
    skipped_out_of_county = 0
    auth_failed = False

    for city in cities:
        if auth_failed:
            break
        url = FOURSQUARE_SEARCH_URL
        params = {
            # NOTE: the new places-api.foursquare.com endpoint silently
            # ignores a "categories" param (it returns unfiltered nearby
            # results with a 200 instead of erroring) — the working filter
            # param is "fsq_category_ids". Confirmed empirically 2026-08-22.
            "fsq_category_ids": FOURSQUARE_INSURANCE_CATEGORY,
            # A county qualifier disambiguates cities that share a name with
            # a place elsewhere in KS — plain "Shawnee, KS" geocodes to
            # Shawnee County/Topeka instead of the Johnson County city of
            # Shawnee. Confirmed empirically 2026-08-22.
            "near": f"{city}, {county_qualifier}, {state}" if county_qualifier else f"{city}, {state}",
            "limit": FOURSQUARE_PAGE_LIMIT,
        }
        page = 0
        while url and page < FOURSQUARE_MAX_PAGES:
            try:
                resp = requests.get(url, headers=headers, params=params, timeout=15)
            except requests.RequestException as e:
                errors.append(f"Foursquare: request error for {city}: {e}")
                break

            if resp.status_code == 401:
                errors.append(
                    f"Foursquare: 401 Unauthorized ({resp.text[:200]}) — API key is "
                    "invalid/expired/revoked. Aborting Foursquare discovery."
                )
                auth_failed = True
                break
            if resp.status_code == 429:
                errors.append(f"Foursquare: 429 rate-limited at {city}. Stopping Foursquare discovery.")
                auth_failed = True
                break
            try:
                resp.raise_for_status()
            except requests.RequestException as e:
                errors.append(f"Foursquare: HTTP error for {city}: {e} — {resp.text[:200]}")
                break

            data = resp.json()
            results = data.get("results", [])
            for place in results:
                loc = place.get("location", {})
                locality = loc.get("locality", "") or city
                if locality.strip().lower() not in city_lookup:
                    # Geocoding drifted out of Johnson County (e.g. a plain
                    # "Shawnee, KS" near-string resolving to Topeka/Shawnee
                    # County) — drop it rather than mislabel the source city.
                    skipped_out_of_county += 1
                    continue
                records.append({
                    "name": place.get("name", "") or "",
                    "phone": place.get("tel", "") or "",
                    "street_address": loc.get("address", "") or "",
                    "city": locality,
                    "zip": loc.get("postcode", "") or "",
                    "website": place.get("website"),
                    "source_url": place.get("placemaker_url") or place.get("link") or "",
                    "source": "Foursquare",
                })

            # Pagination via RFC5988 Link header: <url>; rel="next"
            next_url = None
            link_header = resp.headers.get("Link", "")
            m = re.search(r'<([^>]+)>;\s*rel="next"', link_header)
            if m:
                next_url = m.group(1)

            page += 1
            if not results or not next_url:
                break
            url = next_url
            params = None  # next_url already has all query params encoded
            time.sleep(0.3)

        time.sleep(0.2)

    if skipped_out_of_county:
        errors.append(
            f"Foursquare: skipped {skipped_out_of_county} results whose geocoded "
            f"locality fell outside the {len(cities)} target cities (likely 'near' "
            "geocoder drift)"
        )

    return records


def main():
    keys = load_api_keys()
    errors = []

    print("[insurance/api_discovery] Discovering insurance agents in Johnson County, KS")
    print(f"[insurance/api_discovery] Cities: {len(CITIES)}")

    print("[insurance/api_discovery] Querying Yelp Fusion...")
    yelp_records = discover_yelp(keys["yelp"], errors)
    yelp_deduped = _dedupe(yelp_records)
    print(f"[insurance/api_discovery] Yelp: {len(yelp_records)} raw -> {len(yelp_deduped)} deduped")

    print("[insurance/api_discovery] Querying Foursquare Places...")
    fsq_records = discover_foursquare(keys["foursquare"], errors)
    fsq_deduped = _dedupe(fsq_records)
    print(f"[insurance/api_discovery] Foursquare: {len(fsq_records)} raw -> {len(fsq_deduped)} deduped")

    combined = yelp_deduped + fsq_deduped

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(combined, f, indent=2)

    print(f"[insurance/api_discovery] Wrote {len(combined)} records to {OUTPUT_PATH}")
    if errors:
        print("[insurance/api_discovery] Errors/limitations encountered:")
        for e in errors:
            print(f"  - {e}")

    return {
        "yelp_count": len(yelp_deduped),
        "foursquare_count": len(fsq_deduped),
        "total": len(combined),
        "errors": errors,
    }


if __name__ == "__main__":
    main()
