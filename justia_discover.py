#!/usr/bin/env python3
"""Justia attorney discovery for KC Metro KS counties using chrome120 impersonation.

Justia cards contain: name, phone, website, address (street+city+zip), practice areas.
City-validated via the address div in each card.
"""
import csv, re, sys, time
from pathlib import Path

from curl_cffi import requests as creq
from bs4 import BeautifulSoup

DATA_DIR = Path(__file__).parent / "app/county-data"

FIELDNAMES = [
    "law_firm_name", "website", "google_business_profile", "legal_directory_listing",
    "city", "state", "county", "phone_number", "email", "practice_area",
    "street_address", "zip_code", "msa", "priority", "number_of_lawyers",
]

PRIORITY_MAP = {
    "Personal Injury": 5, "Medical Malpractice": 5, "Workers Compensation": 5,
    "Criminal Defense": 5, "DUI/DWI": 5, "Family": 5, "Divorce": 5,
    "Bankruptcy": 4, "Estate Planning": 4, "Probate": 4, "Real Estate": 4,
    "Business": 4, "Employment": 4, "Immigration": 4,
    "Civil Rights": 3, "Social Security Disability": 3, "Tax": 3,
    "Intellectual Property": 3, "General": 2,
}

PA_SYNONYMS = {
    "Criminal Law": "Criminal Defense", "Criminal Defense": "Criminal Defense",
    "Family Law": "Family", "Divorce": "Divorce",
    "Personal Injury": "Personal Injury", "Medical Malpractice": "Medical Malpractice",
    "Workers' Compensation": "Workers Compensation",
    "Bankruptcy": "Bankruptcy",
    "Estate Planning": "Estate Planning", "Probate": "Probate",
    "Real Estate Law": "Real Estate",
    "Business Law": "Business",
    "Employment Law": "Employment",
    "Immigration Law": "Immigration",
    "Civil Rights": "Civil Rights",
    "Social Security Disability": "Social Security Disability",
    "Tax Law": "Tax",
    "Intellectual Property": "Intellectual Property",
    "DUI": "DUI/DWI", "DUI/DWI": "DUI/DWI",
    "Domestic Violence": "Criminal Defense",
}

COUNTIES = {
    "johnson-county-ks": {
        "county": "Johnson", "state": "KS", "msa": "Kansas City",
        "cities": [
            "Overland Park", "Olathe", "Shawnee", "Lenexa", "Leawood",
            "Prairie Village", "Merriam", "Mission", "Gardner", "Spring Hill",
            "De Soto", "Roeland Park", "Fairway", "Westwood", "Edgerton",
        ],
    },
    "wyandotte-county-ks": {
        "county": "Wyandotte", "state": "KS", "msa": "Kansas City",
        "cities": ["Kansas City", "Bonner Springs", "Edwardsville"],
    },
    "leavenworth-county-ks": {
        "county": "Leavenworth", "state": "KS", "msa": "Kansas City",
        "cities": ["Leavenworth", "Lansing", "Basehor", "Tonganoxie", "Linwood", "Easton"],
    },
    "miami-county-ks": {
        "county": "Miami", "state": "KS", "msa": "Kansas City",
        "cities": ["Paola", "Osawatomie", "Louisburg", "Fontana"],
    },
    "linn-county-ks": {
        "county": "Linn", "state": "KS", "msa": "Kansas City",
        "cities": ["Pleasanton", "La Cygne", "Mound City", "Prescott", "Blue Mound"],
    },
    "douglas-county-ks": {
        "county": "Douglas", "state": "KS", "msa": "Lawrence",
        "cities": ["Lawrence", "Eudora", "Baldwin City", "Lecompton"],
    },
    "franklin-county-ks": {
        "county": "Franklin", "state": "KS", "msa": "Kansas City",
        "cities": ["Ottawa", "Wellsville", "Williamsburg", "Richmond", "Lane"],
    },
    "jefferson-county-ks": {
        "county": "Jefferson", "state": "KS", "msa": "Topeka",
        "cities": ["Oskaloosa", "Winchester", "Valley Falls", "Meriden", "McLouth", "Perry", "Nortonville"],
    },
    "osage-county-ks": {
        "county": "Osage", "state": "KS", "msa": "Topeka",
        "cities": ["Lyndon", "Osage City", "Burlingame", "Overbrook", "Scranton"],
    },
    "shawnee-county-ks": {
        "county": "Shawnee", "state": "KS", "msa": "Topeka",
        "cities": ["Topeka", "Silver Lake", "Rossville", "Willard", "Auburn", "Wakarusa", "Tecumseh"],
    },
}

# Justia city slugs: lowercase, spaces to hyphens
CITY_SLUGS = {city: city.lower().replace(" ", "-").replace("de soto", "de-soto")
              for info in COUNTIES.values() for city in info["cities"]}
# Special overrides
CITY_SLUGS["De Soto"] = "de-soto"
CITY_SLUGS["La Cygne"] = "la-cygne"
CITY_SLUGS["Mound City"] = "mound-city"
CITY_SLUGS["Bonner Springs"] = "bonner-springs"
CITY_SLUGS["Spring Hill"] = "spring-hill"
CITY_SLUGS["Overland Park"] = "overland-park"
CITY_SLUGS["Prairie Village"] = "prairie-village"
CITY_SLUGS["Roeland Park"] = "roeland-park"
CITY_SLUGS["Kansas City"] = "kansas-city"
CITY_SLUGS["Mound City"] = "mound-city"
CITY_SLUGS["Blue Mound"] = "blue-mound"


def normalize(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(r"\b(law|firm|office|offices|group|llc|llp|pc|pa|pllc|plc|&|and|the|attorney|attorneys|at|of|lawyer|lawyers)\b", "", name)
    name = re.sub(r"[^a-z0-9]", "", name)
    return name


def load_existing(county_slug: str) -> tuple[list[dict], set[str]]:
    path = DATA_DIR / f"{county_slug}.csv"
    rows = []
    seen = set()
    if path.exists():
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
                key = normalize(row.get("law_firm_name", "")) + "|" + row.get("city", "").lower().strip()
                seen.add(key)
    return rows, seen


def save_csv(county_slug: str, rows: list[dict]) -> None:
    path = DATA_DIR / f"{county_slug}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def parse_address(addr_el) -> tuple[str, str, str, str]:
    """Parse address div → (street, city, state, zip)."""
    if not addr_el:
        return "", "", "", ""
    text = addr_el.get_text(separator="|", strip=True)
    # Format: "8000 Foster St.|Overland Park,  KS 66204"
    parts = [p.strip() for p in text.split("|") if p.strip()]
    street = ""
    city = ""
    state = ""
    zipcode = ""
    if len(parts) >= 2:
        street = parts[0]
        loc = parts[-1]  # "Overland Park,  KS 66204"
        m = re.match(r"^(.+?),\s*([A-Z]{2})\s*(\d{5})?", loc)
        if m:
            city = m.group(1).strip()
            state = m.group(2).strip()
            zipcode = (m.group(3) or "").strip()
    elif len(parts) == 1:
        loc = parts[0]
        m = re.match(r"^(.+?),\s*([A-Z]{2})\s*(\d{5})?", loc)
        if m:
            city = m.group(1).strip()
            state = m.group(2).strip()
            zipcode = (m.group(3) or "").strip()
    return street, city, state, zipcode


def extract_cards(soup, target_cities_lower: set[str]) -> list[dict]:
    """Extract attorney data from Justia listing page, city-validated."""
    results = []
    cards = soup.find_all("div", class_=re.compile(r"jld-card"))
    for card in cards:
        # Name
        name_el = card.find("strong", class_="name")
        if not name_el:
            name_el = card.find("strong", class_=re.compile("name"))
        if not name_el:
            continue
        name_link = name_el.find("a")
        name = name_link.get_text(strip=True) if name_link else name_el.get_text(strip=True)
        if not name:
            continue

        # Address (for city validation)
        addr_el = card.find("div", class_=re.compile("address"))
        street, city, state, zipcode = parse_address(addr_el)

        if not city or city.lower() not in target_cities_lower:
            continue
        if state and state.upper() != "KS":
            continue

        # Phone
        phone_el = card.find("strong", class_="phone")
        phone = ""
        if phone_el:
            phone_link = phone_el.find("a", href=re.compile(r"^tel:"))
            if phone_link:
                phone = phone_link.get_text(strip=True)

        # Website
        website = ""
        for link in card.find_all("a", href=True):
            data_btn = link.get("data-button-tag", "")
            if data_btn == "website":
                href = link.get("href", "")
                if href.startswith("http") and "justia.com" not in href:
                    website = href
                    break

        # Practice areas
        pa_line = ""
        for el in card.find_all(class_=re.compile("iconed-line")):
            text = el.get_text(strip=True)
            if re.search(r"Attorney|lawyer|experience", text, re.I):
                continue
            if re.search(r"school|university|college", text, re.I):
                continue
            if len(text) > 3 and not text.startswith("Free") and not text.startswith("Offers"):
                # Likely practice area line
                pa_line = text
                break

        # Map practice area
        pa = "General"
        priority = 2
        if pa_line:
            for kw, mapped in PA_SYNONYMS.items():
                if kw.lower() in pa_line.lower():
                    pa = mapped
                    priority = PRIORITY_MAP.get(mapped, 2)
                    break

        # Profile URL
        profile_link = card.find("a", href=re.compile(r"lawyers\.justia\.com/lawyer/"))
        profile_url = profile_link.get("href", "") if profile_link else ""

        results.append({
            "name": name,
            "phone": phone,
            "website": website,
            "street": street,
            "city": city,
            "state": state,
            "zip": zipcode,
            "practice_area": pa,
            "priority": priority,
            "profile_url": profile_url,
        })
    return results


def scrape_city(session, city: str, county_info: dict, delay: float = 0.5) -> list[dict]:
    slug = CITY_SLUGS.get(city, city.lower().replace(" ", "-"))
    base_url = f"https://www.justia.com/lawyers/kansas/{slug}"
    target_cities_lower = {c.lower() for c in county_info["cities"]}
    results = []
    page = 1

    while True:
        url = base_url if page == 1 else f"{base_url}?page={page}"
        try:
            r = session.get(url, timeout=20)
            if r.status_code == 404:
                break
            if r.status_code == 429:
                print(f"  {city} p{page}: rate limited, waiting 60s")
                time.sleep(60)
                r = session.get(url, timeout=20)
                if r.status_code != 200:
                    break
            if r.status_code != 200:
                print(f"  {city} p{page}: HTTP {r.status_code}, stopping")
                break

            soup = BeautifulSoup(r.text, "lxml")
            cards = extract_cards(soup, target_cities_lower)
            if not cards:
                break
            results.extend(cards)

            # Check if there's a next page
            page_links = re.findall(r"page=(\d+)", r.text)
            max_page = max(int(p) for p in page_links) if page_links else page
            if page >= max_page:
                break
            page += 1
            time.sleep(delay)

        except Exception as e:
            print(f"  Error on {city} p{page}: {e}")
            break

    print(f"  {city}: {page} pages → {len(results)} valid attorneys")
    return results


def run_county(county_slug: str, session) -> int:
    info = COUNTIES[county_slug]
    print(f"\n{'='*60}")
    print(f"  {county_slug}")
    print(f"{'='*60}")

    existing_rows, seen = load_existing(county_slug)
    before = len(existing_rows)
    added = 0

    for city in info["cities"]:
        attorneys = scrape_city(session, city, info)
        for a in attorneys:
            key = normalize(a["name"]) + "|" + a["city"].lower().strip()
            if key in seen or not normalize(a["name"]):
                continue
            seen.add(key)
            existing_rows.append({
                "law_firm_name": a["name"],
                "website": a.get("website", ""),
                "google_business_profile": "",
                "legal_directory_listing": a.get("profile_url", ""),
                "city": a["city"],
                "state": info["state"],
                "county": info["county"],
                "phone_number": a.get("phone", ""),
                "email": "",
                "practice_area": a.get("practice_area", "General"),
                "street_address": a.get("street", ""),
                "zip_code": a.get("zip", ""),
                "msa": info["msa"],
                "priority": str(a.get("priority", 2)),
                "number_of_lawyers": "",
            })
            added += 1

    if added > 0:
        save_csv(county_slug, existing_rows)

    print(f"  {county_slug}: {before} → {before + added} (+{added} new)")
    return added


def main():
    target_slugs = list(COUNTIES.keys())
    if len(sys.argv) > 1:
        target_slugs = [s for s in sys.argv[1:] if s in COUNTIES]

    session = creq.Session(impersonate="chrome120")
    total_added = 0

    for slug in target_slugs:
        added = run_county(slug, session)
        total_added += added

    print(f"\nTotal new attorneys added: {total_added}")


if __name__ == "__main__":
    main()
