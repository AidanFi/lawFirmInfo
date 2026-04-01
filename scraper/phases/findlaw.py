"""FindLaw scraper — crawl practice area / city listing pages for Kansas law firms.

Uses curl_cffi to bypass Cloudflare protection. Extracts firm name, phone,
and practice area associations directly from listing cards (no profile visits).

Rate limited to ~1 request per second.
"""
import re
import time

from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests

from scraper.utils.normalize import normalize_firm_name, are_same_firm

_BASE = "https://lawyers.findlaw.com"
_IMPERSONATE = "chrome"

# Focus on the most common/valuable practice areas
_TOP_PRACTICE_AREAS = [
    ("Personal Injury", "personal-injury-plaintiff"),
    ("Family Law", "family-law"),
    ("Criminal Defense", "criminal-law"),
    ("Estate Planning", "estate-planning"),
    ("Business Law", "business-commercial-law"),
    ("Real Estate", "real-estate"),
    ("Bankruptcy", "bankruptcy-law"),
    ("Employment Law", "employment-labor-law"),
    ("Workers' Compensation", "workers-compensation"),
    ("DUI/DWI", "dui-dwi"),
    ("Immigration", "immigration"),
    ("Divorce", "divorce"),
    ("Medical Malpractice", "medical-malpractice"),
    ("Civil Litigation", "general-litigation"),
    ("Tax Law", "tax-law"),
    ("Intellectual Property", "intellectual-property"),
    ("Elder Law", "elder-law"),
    ("Insurance", "insurance-law"),
    ("Social Security Disability", "social-security-disability"),
    ("Probate", "probate-estate-administration"),
    ("Child Custody", "custody-visitation"),
    ("Wrongful Death", "wrongful-death"),
    ("Traffic Violations", "traffic-violations"),
    ("Trusts", "trusts"),
    ("Wills", "wills"),
    ("Construction", "construction-law"),
    ("Environmental Law", "environmental-law"),
    ("Civil Rights", "civil-rights"),
    ("Consumer Protection", "consumer-protection"),
    ("Government", "government"),
]


def _get(url: str, delay: float = 1.0) -> BeautifulSoup | None:
    try:
        r = cffi_requests.get(url, impersonate=_IMPERSONATE, timeout=15)
        if r.status_code != 200:
            return None
        time.sleep(delay)
        return BeautifulSoup(r.text, "lxml")
    except Exception:
        return None


def _get_city_slugs(pa_slug: str, delay: float) -> list[str]:
    """Get city URL slugs for a practice area."""
    url = f"{_BASE}/{pa_slug}/kansas/"
    soup = _get(url, delay=delay)
    if not soup:
        return []
    slugs = []
    prefix = f"/{pa_slug}/kansas/"
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if prefix in href and href.rstrip("/") != f"{_BASE}{prefix}".rstrip("/"):
            # Extract city slug from URL
            m = re.search(rf"{re.escape(prefix)}([\w-]+)/?", href)
            if m and m.group(1) not in slugs:
                slugs.append(m.group(1))
    return slugs


def _extract_firms_from_listing(soup: BeautifulSoup, practice_area: str,
                                city_slug: str) -> list[dict]:
    """Extract firm entries from a listing page."""
    firms = []
    seen = set()
    city = city_slug.replace("-", " ").title()

    cards = soup.find_all(class_="fl-serp-card")
    for card in cards:
        title_el = card.find(class_="fl-serp-card-title")
        if not title_el:
            continue
        name = re.sub(r"Sponsored$", "", title_el.get_text(strip=True)).strip()
        if not name:
            continue

        # Address from location element
        street = ""
        loc_el = card.find(class_="fl-serp-card-location")
        if loc_el:
            loc_text = loc_el.get_text(strip=True)
            # Pattern: "street, City, ST ZIP"
            parts = loc_text.rsplit(",", 2)
            if len(parts) >= 2:
                street = parts[0].strip()

        # Deduplicate within page
        key = normalize_firm_name(name)
        if key in seen:
            continue
        seen.add(key)

        firms.append({
            "name": name,
            "phone": None,
            "street": street,
            "practiceAreas": [practice_area],
            "city": city,
        })

    return firms


def scrape_findlaw(delay: float = 1.0, test_mode: bool = False) -> list[dict]:
    """Scrape FindLaw Kansas directory. Returns list of firm dicts."""
    pa_list = _TOP_PRACTICE_AREAS
    if test_mode:
        pa_list = pa_list[:3]

    print(f"[findlaw] Crawling {len(pa_list)} practice areas...")

    # Accumulate firms: key -> {name, phone, practiceAreas, city}
    firm_map: dict[str, dict] = {}  # normalized_name|city -> firm
    pages_scraped = 0

    for pa_idx, (pa_name, pa_slug) in enumerate(pa_list):
        city_slugs = _get_city_slugs(pa_slug, delay=delay)
        if test_mode:
            city_slugs = city_slugs[:3]

        for city_slug in city_slugs:
            url = f"{_BASE}/{pa_slug}/kansas/{city_slug}/"
            page = 1
            while url and page <= 10:
                soup = _get(url, delay=delay)
                if not soup:
                    break

                entries = _extract_firms_from_listing(soup, pa_name, city_slug)
                for entry in entries:
                    key = normalize_firm_name(entry["name"]) + "|" + entry["city"].lower()
                    if key in firm_map:
                        # Add practice area
                        existing_pa = set(firm_map[key]["practiceAreas"])
                        for pa in entry["practiceAreas"]:
                            if pa not in existing_pa:
                                firm_map[key]["practiceAreas"].append(pa)
                        if not firm_map[key].get("phone") and entry.get("phone"):
                            firm_map[key]["phone"] = entry["phone"]
                        if not firm_map[key].get("street") and entry.get("street"):
                            firm_map[key]["street"] = entry["street"]
                    else:
                        firm_map[key] = entry

                pages_scraped += 1

                # Check for next page
                next_link = soup.find("a", string=re.compile(r"Next", re.I), href=True)
                if next_link:
                    href = next_link["href"]
                    if not href.startswith("http"):
                        href = f"{_BASE}{href}"
                    url = href
                    page += 1
                else:
                    break

        if (pa_idx + 1) % 5 == 0:
            print(f"[findlaw] Progress: {pa_idx+1}/{len(pa_list)} practice areas, "
                  f"{len(firm_map)} unique firms, {pages_scraped} pages")

    firms = list(firm_map.values())
    print(f"[findlaw] Done: {len(firms)} unique firms from {pages_scraped} pages")
    return firms


def merge_findlaw_into_firms(firms: list, findlaw_firms: list) -> list:
    """Merge FindLaw data into existing firm list."""
    import uuid
    from scraper.utils.normalize import _get_base_domain

    merged = 0
    added = 0

    for fl_firm in findlaw_firms:
        matched = False
        fl_name = fl_firm["name"]
        fl_city = fl_firm.get("city", "").lower()

        for firm in firms:
            firm_city = (firm.get("address") or {}).get("city", "").lower()
            same_city = firm_city == fl_city
            if not same_city:
                continue

            same_name = are_same_firm(firm.get("name", ""), fl_name)
            same_phone = (firm.get("phone") and fl_firm.get("phone")
                          and firm["phone"] == fl_firm["phone"])

            if same_name or same_phone:
                # Enrich existing firm
                existing_pa = set(firm.get("practiceAreas") or [])
                for pa in fl_firm.get("practiceAreas", []):
                    if pa not in existing_pa:
                        firm.setdefault("practiceAreas", []).append(pa)
                        existing_pa.add(pa)
                if not firm.get("phone") and fl_firm.get("phone"):
                    firm["phone"] = fl_firm["phone"]
                if fl_firm.get("street") and not (firm.get("address") or {}).get("street"):
                    firm.setdefault("address", {})["street"] = fl_firm["street"]
                if "findlaw" not in firm.get("sources", []):
                    firm.setdefault("sources", []).append("findlaw")
                matched = True
                merged += 1
                break

        if not matched:
            new_firm = {
                "id": str(uuid.uuid4()),
                "name": fl_firm["name"],
                "practiceAreas": fl_firm.get("practiceAreas", []),
                "summary": None,
                "website": None,
                "phone": fl_firm.get("phone"),
                "email": None,
                "address": {"street": fl_firm.get("street", ""),
                            "city": fl_firm.get("city", ""),
                            "county": "", "state": "KS", "zip": ""},
                "coordinates": None,
                "referralScore": "low",
                "sources": ["findlaw"],
            }
            firms.append(new_firm)
            added += 1

    print(f"[findlaw] Merged into {merged} existing firms, added {added} new firms")
    return firms
