import json
import os
import re
import time
import uuid

import requests
from bs4 import BeautifulSoup

from scraper.utils.normalize import (
    are_same_firm, normalize_firm_name, normalize_practice_area,
)
from scraper.phases.website_scraper import scrape_firm_website


_NON_LEGAL_INDICATORS = (
    "bank", "hospital", "medical center", "pharmacy", "restaurant",
    "gas station", "insurance company", "school district", "university",
    "college", "church", "lawn care", "cleaning", "plumbing",
    "roofing", "electric", "heating", "cooling", "hvac",
    "construction", "realty", "real estate group", "mortgage",
    "credit union", "financial group", "investments",
    "manufacturing", "inc.", "corporation", "corp.",
    "county government", "city of ", "state of ",
    "gazette", "news", "media", "broadcasting",
)

_LEGAL_INDICATORS = (
    "law", "legal", "attorney", "lawyer", "counsel", "advocate",
    "firm", "llc", "llp", "pllc", "p.a.", " pa", " pc",
    "chartered", "esquire", "esq",
)


def _looks_like_legal_entity(name: str, practice_areas: list) -> bool:
    lower = name.lower()
    if any(ind in lower for ind in _LEGAL_INDICATORS):
        return True
    if any(ind in lower for ind in _NON_LEGAL_INDICATORS):
        return False
    if practice_areas:
        return True
    # Person names (solo practitioners) — allow through
    return True


def _find_matching_firm(name: str, city: str, firms: list) -> dict | None:
    city_lower = city.lower()
    for firm in firms:
        firm_city = (firm.get("address") or {}).get("city", "").lower()
        if firm_city == city_lower and are_same_firm(name, firm["name"]):
            return firm
    return None


def _add_source(firm: dict, source: str):
    sources = firm.setdefault("sources", [])
    if source not in sources:
        sources.append(source)


# ---------------------------------------------------------------------------
# Sub-step 1: KS Courts cross-check
# ---------------------------------------------------------------------------

def _crosscheck_ks_courts(firms: list, county_config: dict) -> int:
    firms_data_path = os.path.join("app", "firms_data.js")
    if not os.path.exists(firms_data_path):
        print("  [enhance] firms_data.js not found — skipping KS Courts cross-check")
        return 0

    with open(firms_data_path, "r", encoding="utf-8") as f:
        content = f.read()

    match = re.search(r'const\s+FIRMS_DATA\s*=\s*(\{.*\})\s*;?\s*$', content, re.DOTALL)
    if not match:
        print("  [enhance] Could not parse firms_data.js — skipping KS Courts cross-check")
        return 0

    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError:
        print("  [enhance] JSON parse failed for firms_data.js — skipping")
        return 0

    county_cities = {c.lower() for c in county_config["cities"]}
    added = 0

    for existing_firm in data.get("firms", []):
        addr = existing_firm.get("address") or {}
        city = addr.get("city", "")
        if city.lower() not in county_cities:
            continue

        if _find_matching_firm(existing_firm["name"], city, firms):
            continue

        if not _looks_like_legal_entity(
            existing_firm["name"],
            existing_firm.get("practiceAreas", []),
        ):
            continue

        firms.append({
            "id": str(uuid.uuid4()),
            "name": existing_firm["name"],
            "practiceAreas": existing_firm.get("practiceAreas", []),
            "summary": existing_firm.get("summary"),
            "website": existing_firm.get("website"),
            "phone": existing_firm.get("phone"),
            "email": existing_firm.get("email"),
            "address": addr,
            "coordinates": existing_firm.get("coordinates"),
            "sources": ["ks_courts_crosscheck"],
            "google_business_profile": "",
        })
        added += 1

    print(f"  [enhance] KS Courts cross-check: added {added} firms not found by APIs")
    return added


# ---------------------------------------------------------------------------
# Sub-step 2: Martindale enrichment + URL capture
# ---------------------------------------------------------------------------

def _enrich_martindale(firms: list, county_config: dict) -> int:
    try:
        from scraper.phases.martindale import scrape_martindale
    except ImportError:
        print("  [enhance] martindale module not available — skipping")
        return 0

    cities = county_config["cities"]
    before = len(firms)
    try:
        added_websites, new_firms = scrape_martindale(
            firms, cities=cities, delay=1.5, max_pages_per_city=5, add_new=True,
        )
    except Exception as e:
        print(f"  [enhance] Martindale error: {e}")
        return 0

    for firm in firms:
        if "martindale" in (firm.get("sources") or []):
            if not firm.get("martindale_url"):
                name_slug = normalize_firm_name(firm["name"]).replace(" ", "-")
                firm["martindale_url"] = f"https://www.martindale.com/by-location/kansas-lawyers/"

    print(f"  [enhance] Martindale: +{added_websites} websites, +{new_firms} new firms")
    return added_websites + new_firms


# ---------------------------------------------------------------------------
# Sub-step 3: Justia search for directory URLs
# ---------------------------------------------------------------------------

_JUSTIA_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; LawFirmDirectory/1.0)"}


def _enrich_justia(firms: list, county_config: dict) -> int:
    enriched = 0
    state_lower = county_config["state"].lower()
    state_map = {"ks": "kansas"}
    state_name = state_map.get(state_lower, state_lower)

    for city in county_config["cities"]:
        city_slug = city.lower().replace(" ", "-")
        url = f"https://www.justia.com/lawyers/{state_name}/{city_slug}"

        try:
            resp = requests.get(url, headers=_JUSTIA_HEADERS, timeout=15)
            if resp.status_code != 200:
                continue
        except requests.RequestException:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            if "/lawyer/" not in href and "/law-firm/" not in href:
                continue
            name_text = link.get_text(strip=True)
            if not name_text or len(name_text) < 3:
                continue

            firm = _find_matching_firm(name_text, city, firms)
            if firm and not firm.get("justia_url"):
                full_url = href if href.startswith("http") else f"https://www.justia.com{href}"
                firm["justia_url"] = full_url
                _add_source(firm, "justia")
                enriched += 1

        time.sleep(1.0)

    print(f"  [enhance] Justia: captured {enriched} directory URLs")
    return enriched


# ---------------------------------------------------------------------------
# Sub-step 4: Avvo search for directory URLs
# ---------------------------------------------------------------------------

def _enrich_avvo(firms: list, county_config: dict) -> int:
    enriched = 0
    for city in county_config["cities"]:
        city_slug = city.lower().replace(" ", "-")
        url = f"https://www.avvo.com/all-lawyers/{city_slug}-ks.html"

        try:
            resp = requests.get(url, headers=_JUSTIA_HEADERS, timeout=15)
            if resp.status_code != 200:
                continue
        except requests.RequestException:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            if "/attorneys/" not in href:
                continue
            name_text = link.get_text(strip=True)
            if not name_text or len(name_text) < 3:
                continue

            firm = _find_matching_firm(name_text, city, firms)
            if firm and not firm.get("avvo_url"):
                full_url = href if href.startswith("http") else f"https://www.avvo.com{href}"
                firm["avvo_url"] = full_url
                _add_source(firm, "avvo")
                enriched += 1

        time.sleep(1.0)

    print(f"  [enhance] Avvo: captured {enriched} directory URLs")
    return enriched


# ---------------------------------------------------------------------------
# Sub-step 5: FindLaw search for directory URLs
# ---------------------------------------------------------------------------

def _enrich_findlaw(firms: list, county_config: dict) -> int:
    enriched = 0
    for city in county_config["cities"]:
        city_slug = city.lower().replace(" ", "+")
        url = f"https://lawyers.findlaw.com/lawyer/firm/practice/{county_config['state']}/{city_slug}"

        try:
            resp = requests.get(url, headers=_JUSTIA_HEADERS, timeout=15)
            if resp.status_code != 200:
                continue
        except requests.RequestException:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            if "/profile/" not in href and "/firm/" not in href:
                continue
            name_text = link.get_text(strip=True)
            if not name_text or len(name_text) < 3:
                continue

            firm = _find_matching_firm(name_text, city, firms)
            if firm and not firm.get("findlaw_url"):
                full_url = href if href.startswith("http") else f"https://lawyers.findlaw.com{href}"
                firm["findlaw_url"] = full_url
                _add_source(firm, "findlaw")
                enriched += 1

        time.sleep(1.0)

    print(f"  [enhance] FindLaw: captured {enriched} directory URLs")
    return enriched


# ---------------------------------------------------------------------------
# Sub-step 6: Website scraping for emails and practice areas
# ---------------------------------------------------------------------------

def _scrape_websites(firms: list) -> int:
    to_scrape = [f for f in firms if f.get("website") and not f.get("email")]
    scraped = 0

    for firm in to_scrape:
        try:
            result = scrape_firm_website(
                firm["website"], firm["name"],
                (firm.get("address") or {}).get("city", "")
            )
        except Exception:
            continue

        if result.get("email"):
            firm["email"] = result["email"]
        if result.get("summary") and not firm.get("summary"):
            firm["summary"] = result["summary"]
        if result.get("practiceAreas"):
            existing = set(firm.get("practiceAreas") or [])
            for area in result["practiceAreas"]:
                normalized = normalize_practice_area(area)
                if normalized not in existing:
                    firm.setdefault("practiceAreas", []).append(normalized)
                    existing.add(normalized)
        _add_source(firm, "website_scraper")
        scraped += 1

        if scraped % 25 == 0:
            print(f"  [enhance] Website scraping progress: {scraped}/{len(to_scrape)}")
        time.sleep(1.0)

    print(f"  [enhance] Website scraping: processed {scraped} sites")
    return scraped


# ---------------------------------------------------------------------------
# Main enhancement coordinator
# ---------------------------------------------------------------------------

def enhance_firms(firms: list, county_config: dict, test_mode: bool = False) -> list:
    print(f"\n[enhance] Starting enhancement for {county_config['name']}...")
    print(f"[enhance] {len(firms)} firms to enhance\n")

    _crosscheck_ks_courts(firms, county_config)

    if not test_mode:
        _enrich_martindale(firms, county_config)
        _enrich_justia(firms, county_config)
        _enrich_avvo(firms, county_config)
        _enrich_findlaw(firms, county_config)
        _scrape_websites(firms)
    else:
        print("  [enhance] Test mode — skipping directory enrichment and website scraping")

    # Select best legal directory listing per firm
    for firm in firms:
        if not firm.get("legal_directory_listing"):
            for key in ("martindale_url", "justia_url", "avvo_url", "findlaw_url"):
                url = firm.get(key)
                if url:
                    firm["legal_directory_listing"] = url
                    break

    # Set county on firms whose city is in the county; filter out the rest
    county_name = county_config["name"].replace(" County", "")
    county_cities_lower = {c.lower() for c in county_config["cities"]}
    filtered = []
    for firm in firms:
        addr = firm.setdefault("address", {})
        firm_city = addr.get("city", "").lower()
        if not firm_city or firm_city not in county_cities_lower:
            continue
        if not addr.get("county"):
            addr["county"] = county_name
        filtered.append(firm)
    removed = len(firms) - len(filtered)
    if removed:
        print(f"  [enhance] Removed {removed} firms outside {county_config['name']}")
    firms = filtered

    enriched_with_email = sum(1 for f in firms if f.get("email"))
    enriched_with_directory = sum(1 for f in firms if f.get("legal_directory_listing"))
    print(f"\n[enhance] Enhancement complete:")
    print(f"  Total firms: {len(firms)}")
    print(f"  With email: {enriched_with_email}")
    print(f"  With directory listing: {enriched_with_directory}")

    return firms
