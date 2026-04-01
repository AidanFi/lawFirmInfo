import re, time, uuid
from scraper.utils.normalize import normalize_firm_name, are_same_firm

KANSAS_CITIES = [
    "Wichita", "Overland Park", "Kansas City", "Topeka", "Olathe",
    "Lawrence", "Shawnee", "Manhattan", "Lenexa", "Salina",
    "Hutchinson", "Leavenworth", "Leawood", "Dodge City", "Garden City",
    "Emporia", "Derby", "Liberal", "Junction City", "Hays",
    "Pittsburg", "Gardner", "Prairie Village", "Lansing", "Merriam",
    "Newton", "Great Bend", "McPherson", "El Dorado", "Ottawa",
    "Coffeyville", "Chanute", "Parsons", "Atchison", "Winfield",
    "Augusta", "Haysville", "Andover", "Bel Aire", "Abilene",
    "Pratt", "Independence", "Colby", "Fort Scott", "Ulysses",
    "Wellington", "Arkansas City", "Iola", "Paola", "Eureka",
    "Concordia", "Ness City", "Stockton", "Smith Center", "Oberlin",
    "Phillipsburg", "Osborne", "Norton", "Almena", "Mankato",
    "Minneola", "Sublette", "Hugoton", "Liberal", "Scott City",
]

COUNTY_SEATS_105 = [
    "Abilene", "Alma", "Almena", "Anthony", "Arkansas City",
    "Ashland", "Atchison", "Belleville", "Beloit",
    "Burlington", "Caldwell", "Chanute", "Cimarron", "Clay Center",
    "Coldwater", "Columbus", "Concordia", "Cottonwood Falls", "Council Grove",
    "Dodge City", "El Dorado", "Elkhart", "Ellsworth", "Emporia",
    "Erie", "Eureka", "Fort Scott", "Fredonia", "Garden City",
    "Garnett", "Goodland", "Great Bend", "Greensburg", "Hays",
    "Hiawatha", "Hill City", "Hillsboro", "Holton", "Howard",
    "Hoxie", "Hugoton", "Hutchinson", "Independence", "Iola",
    "Jetmore", "Johnson", "Junction City", "Kansas City", "Kingman",
    "Kinsley", "La Crosse", "Lakin", "Larned", "Lawrence",
    "Leavenworth", "Leoti", "Liberal", "Lincoln", "Lindsborg",
    "Logan", "Lyndon", "Lyons", "Madison", "Manhattan",
    "Mankato", "Marion", "Marysville", "McPherson", "Meade",
    "Medicine Lodge", "Minneapolis", "Mound City", "Ness City", "Newton",
    "Norton", "Oakley", "Oberlin", "Olathe", "Osborne",
    "Oskaloosa", "Oswego", "Ottawa", "Paola",
    "Phillipsburg", "Pittsburg", "Pratt", "Russell", "Salina",
    "Sedan", "Seneca", "Smith Center", "St. John", "Stockton",
    "Sublette", "Syracuse", "Topeka", "Tribune", "Troy",
    "Ulysses", "Wakeeney", "Washington", "Wellington", "Wichita",
    "Winfield", "Yates Center",
]

_ZIP_RE = re.compile(r'\b(\d{5})\b')
_STATE_RE = re.compile(r',\s*([A-Z]{2})\s+\d{5}')
_CITY_RE = re.compile(r',\s*([^,]+),\s*[A-Z]{2}\s+\d{5}')


def _parse_address(formatted: str) -> dict:
    parts = [p.strip() for p in formatted.split(",")]
    zip_match = _ZIP_RE.search(formatted)
    state_match = _STATE_RE.search(formatted)
    city_match = _CITY_RE.search(formatted)
    return {
        "street": parts[0] if parts else "",
        "city": city_match.group(1).strip() if city_match else "",
        "county": "",
        "state": state_match.group(1) if state_match else "KS",
        "zip": zip_match.group(1) if zip_match else "",
    }


def _is_duplicate(name: str, city: str, existing: list) -> bool:
    for firm in existing:
        if firm["address"]["city"].lower() == city.lower():
            if are_same_firm(name, firm["name"]):
                return True
    return False


def scrape_google_places(client, cities: list = None, delay: float = 0.5) -> list:
    if cities is None:
        cities = KANSAS_CITIES
    firms = []
    for city in cities:
        query = f"law firm in {city} Kansas"
        try:
            response = client.places(query=query)
        except Exception as e:
            print(f"[places] Error searching {city}: {e}")
            continue

        page_results = response.get("results", [])
        next_token = response.get("next_page_token")

        while True:
            for place in page_results:
                name = place.get("name", "")
                address = _parse_address(place.get("formatted_address", ""))
                city_val = address["city"] or city

                if _is_duplicate(name, city_val, firms):
                    continue

                try:
                    details = client.place(place_id=place["place_id"], fields=[
                        "name", "formatted_address", "formatted_phone_number",
                        "website", "geometry"
                    ])["result"]
                except Exception:
                    details = place

                address = _parse_address(details.get("formatted_address", place.get("formatted_address", "")))
                loc = details.get("geometry", {}).get("location", {})

                firms.append({
                    "id": str(uuid.uuid4()),
                    "name": details.get("name", name),
                    "practiceAreas": [],
                    "summary": None,
                    "website": details.get("website"),
                    "phone": details.get("formatted_phone_number"),
                    "email": None,
                    "address": address,
                    "coordinates": {"lat": loc["lat"], "lng": loc["lng"]} if loc else None,
                    "referralScore": "low",
                    "sources": ["google_places"],
                })
                time.sleep(0.1)

            if not next_token:
                break
            time.sleep(3)  # Google requires >=2s; 3s used for reliability
            try:
                response = client.places(page_token=next_token)
                page_results = response.get("results", [])
                next_token = response.get("next_page_token")
            except Exception:
                break

        time.sleep(delay)
    return firms


def merge_google_into_firms(firms: list, google_firms: list) -> list:
    """Merge Google Places results into an existing firm list (enrichment mode).

    For each google_firm, try to find a fuzzy match in *firms* (same city +
    are_same_firm).  When a match is found the existing record is enriched
    with Google data; otherwise the google_firm is appended as a new entry.

    Returns the updated *firms* list (mutated in place for efficiency).
    """
    for gf in google_firms:
        g_city = (gf.get("address") or {}).get("city", "").lower()
        matched = False
        for firm in firms:
            f_city = (firm.get("address") or {}).get("city", "").lower()
            if f_city == g_city and are_same_firm(gf["name"], firm["name"]):
                # Coordinates: Google always wins
                if gf.get("coordinates"):
                    firm["coordinates"] = gf["coordinates"]
                # Phone: Google wins only when existing is None
                if firm.get("phone") is None and gf.get("phone") is not None:
                    firm["phone"] = gf["phone"]
                # Website: Google wins only when existing is None
                if firm.get("website") is None and gf.get("website") is not None:
                    firm["website"] = gf["website"]
                # Address street/zip: Google wins when existing is empty
                g_addr = gf.get("address") or {}
                f_addr = firm.get("address") or {}
                if not f_addr.get("street") and g_addr.get("street"):
                    f_addr["street"] = g_addr["street"]
                if not f_addr.get("zip") and g_addr.get("zip"):
                    f_addr["zip"] = g_addr["zip"]
                # Add source
                sources = firm.setdefault("sources", [])
                if "google_places" not in sources:
                    sources.append("google_places")
                matched = True
                break
        if not matched:
            firms.append(gf)
    return firms
