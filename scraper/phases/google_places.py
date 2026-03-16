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
