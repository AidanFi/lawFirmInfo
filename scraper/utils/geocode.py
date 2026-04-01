from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter


def geocode_firms(firms: list, delay: float = 1.1) -> list:
    """Geocode firms that have an address but no GPS coordinates.

    Modifies firms in place and returns the list.  Uses the free
    Nominatim (OpenStreetMap) API via geopy with built-in rate limiting.

    Strategy: geocode cities first (much fewer unique cities than firms),
    then try full address for better precision where possible.
    """
    geolocator = Nominatim(user_agent="KansasLawFirmDirectory/2.0", timeout=10)
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=delay,
                          max_retries=2, error_wait_seconds=5.0)

    # Phase 1: Build city cache (geocode each unique city once)
    cities_needed = set()
    for firm in firms:
        if firm.get("coordinates"):
            continue
        city = (firm.get("address") or {}).get("city", "").strip()
        if city:
            cities_needed.add(city)

    print(f"[geocode] Geocoding {len(cities_needed)} unique cities...")
    city_cache: dict[str, tuple[float, float] | None] = {}

    for i, city in enumerate(sorted(cities_needed)):
        city_key = city.lower()
        try:
            location = geocode(f"{city}, Kansas")
            if location:
                city_cache[city_key] = (location.latitude, location.longitude)
            else:
                city_cache[city_key] = None
        except Exception:
            city_cache[city_key] = None

        if (i + 1) % 50 == 0:
            print(f"[geocode] Cities: {i + 1}/{len(cities_needed)}")

    geocoded_cities = sum(1 for v in city_cache.values() if v is not None)
    print(f"[geocode] Geocoded {geocoded_cities}/{len(cities_needed)} cities")

    # Phase 2: Assign coordinates to firms
    assigned = 0
    for i, firm in enumerate(firms):
        if firm.get("coordinates"):
            continue

        addr = firm.get("address") or {}
        city = addr.get("city", "").strip()
        if not city:
            continue

        city_key = city.lower()
        cached = city_cache.get(city_key)

        # Try full address for better precision if street is available
        street = addr.get("street", "").strip()
        if street:
            state = addr.get("state", "KS")
            zipcode = addr.get("zip", "")
            full_query = f"{street}, {city}, {state} {zipcode}".strip()
            try:
                location = geocode(full_query)
                if location:
                    firm["coordinates"] = {"lat": location.latitude, "lng": location.longitude}
                    assigned += 1
                    if assigned % 100 == 0:
                        print(f"[geocode] Assigned coordinates to {assigned} firms...")
                    continue
            except Exception:
                pass

        # Fall back to city-level
        if cached is not None:
            firm["coordinates"] = {"lat": cached[0], "lng": cached[1]}
            assigned += 1
            if assigned % 100 == 0:
                print(f"[geocode] Assigned coordinates to {assigned} firms...")

    print(f"[geocode] Total: assigned coordinates to {assigned} firms")
    return firms
