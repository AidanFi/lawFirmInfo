"""Avvo scraper — crawl city listing pages for Kansas attorneys/firms.

Uses curl_cffi to bypass Cloudflare protection. Collects individual
attorney listings and groups them into firms.

Rate limited to ~1 request per second.
"""
import re
import time
import uuid

from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests

from scraper.utils.normalize import normalize_firm_name, are_same_firm

_BASE = "https://www.avvo.com"
_IMPERSONATE = "chrome"


def _get(url: str, delay: float = 1.0) -> BeautifulSoup | None:
    try:
        r = cffi_requests.get(url, impersonate=_IMPERSONATE, timeout=15)
        if r.status_code != 200:
            return None
        time.sleep(delay)
        return BeautifulSoup(r.text, "lxml")
    except Exception:
        return None


def _get_city_urls() -> list[tuple[str, str]]:
    """Return [(city_name, url), ...] for Kansas."""
    soup = _get(f"{_BASE}/all-lawyers/ks.html")
    if not soup:
        return []
    seen_urls = set()
    results = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/all-lawyers/ks/" in href and href != "/all-lawyers/ks.html":
            city = a.get_text(strip=True)
            url = href if href.startswith("http") else f"{_BASE}{href}"
            if city and url not in seen_urls:
                seen_urls.add(url)
                results.append((city, url))
    return results


def _parse_listing_page(soup: BeautifulSoup) -> list[dict]:
    """Extract attorney entries from a single listing page."""
    entries = []
    cards = soup.find_all(class_=lambda c: c and "organic-card" in
                          (c if isinstance(c, str) else " ".join(c)))
    for card in cards:
        entry = {}

        # Attorney name — try the dedicated class first, then fall back
        # to the first link pointing to /attorneys/ with a plausible name
        name_el = card.find(class_=lambda c: c and "search-result-lawyer-name" in
                            (c if isinstance(c, str) else " ".join(c)))
        if name_el:
            entry["attorney_name"] = name_el.get_text(strip=True)

        if not entry.get("attorney_name"):
            # Fallback: first link to /attorneys/ with name-like text
            for a in card.find_all("a", href=True):
                if "/attorneys/" in a.get("href", ""):
                    text = a.get_text(strip=True)
                    # Name-like: 2+ words, no digits, reasonable length
                    if (text and len(text) > 3 and len(text) < 60
                            and not re.search(r"\d", text)
                            and " " in text):
                        entry["attorney_name"] = text
                        break

        # Profile link
        profile_link = None
        if name_el:
            profile_link = name_el.find("a", href=True)
        if not profile_link:
            profile_link = card.find("a", href=lambda h: h and "/attorneys/" in h)
        if profile_link:
            entry["profile_url"] = profile_link["href"]
            if not entry["profile_url"].startswith("http"):
                entry["profile_url"] = f"{_BASE}{entry['profile_url']}"

        # Practice area — usually shown as a subtitle
        pa_el = card.find(class_=lambda c: c and "practice" in str(c).lower())
        if pa_el:
            raw = pa_el.get_text(strip=True)
            # Clean up "Practice Areas:\xa0\n  Child Custody, Family Law and more"
            raw = re.sub(r"^Practice Areas?:?\s*", "", raw, flags=re.I)
            raw = raw.replace("\xa0", " ").strip()
            raw = re.sub(r"\s+and more$", "", raw, flags=re.I)
            entry["practice_areas"] = [pa.strip() for pa in raw.split(",") if pa.strip()]

        # Phone — prefer tel: link for clean number
        tel_link = card.find("a", href=lambda h: h and "tel:" in str(h))
        if tel_link:
            raw_phone = tel_link["href"].replace("tel:", "").strip()
            if len(raw_phone) == 10:
                entry["phone"] = f"({raw_phone[:3]}) {raw_phone[3:6]}-{raw_phone[6:]}"
            else:
                entry["phone"] = raw_phone
        else:
            phone_el = card.find(class_=lambda c: c and "phone-copy" in
                                 (c if isinstance(c, str) else " ".join(c)))
            if phone_el:
                phone_match = re.search(r"\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}",
                                        phone_el.get_text(strip=True))
                if phone_match:
                    entry["phone"] = phone_match.group()

        # Address from card text (pattern: "123 Street, City, ST")
        card_text = card.get_text(separator=" | ", strip=True)
        addr_match = re.search(r"(\d+[^|]+),\s*(\w[\w\s]*),\s*KS", card_text)
        if addr_match:
            entry["street"] = addr_match.group(1).strip()

        if entry.get("attorney_name"):
            entries.append(entry)

    return entries


def _scrape_city(city_name: str, city_url: str, delay: float,
                 max_pages: int = 100) -> list[dict]:
    """Scrape all pages for a city, return attorney entries."""
    all_entries = []
    url = city_url
    page = 1

    while url and page <= max_pages:
        soup = _get(url, delay=delay)
        if not soup:
            break

        entries = _parse_listing_page(soup)
        if not entries and page > 1:
            break
        all_entries.extend(entries)

        # Pagination
        next_link = soup.find("a", href=True, string=re.compile(r"^\d+$"))
        # Find the highest page link to know max pages
        page_links = soup.find_all("a", href=lambda h: h and "page=" in str(h))
        max_page_num = 1
        for pl in page_links:
            try:
                num = int(pl.get_text(strip=True))
                max_page_num = max(max_page_num, num)
            except ValueError:
                pass

        page += 1
        if page <= max_page_num:
            url = re.sub(r"\?page=\d+", "", city_url.rstrip("/"))
            url = f"{url}?page={page}"
        else:
            break

    return all_entries


def _group_into_firms(entries: list[dict], city: str) -> list[dict]:
    """Group attorney entries into firm-level records."""
    # Many Avvo entries are individual attorneys, not firms.
    # We'll keep each attorney as a separate entry since the user wants
    # to maximize count. Practice area and phone data are per-attorney.
    firms = []
    for entry in entries:
        name = entry.get("attorney_name", "").strip()
        if not name:
            continue

        practice_areas = entry.get("practice_areas", [])
        if not practice_areas and entry.get("practice_area"):
            practice_areas = [entry["practice_area"]]

        firm = {
            "name": name,
            "phone": entry.get("phone"),
            "website": None,
            "practiceAreas": practice_areas,
            "address": {"street": entry.get("street", ""), "city": city,
                        "county": "", "state": "KS", "zip": ""},
            "source": "avvo",
            "profile_url": entry.get("profile_url"),
        }
        firms.append(firm)

    return firms


def scrape_avvo(delay: float = 1.0, test_mode: bool = False) -> list[dict]:
    """Scrape Avvo Kansas directory. Returns list of attorney/firm dicts."""
    print("[avvo] Getting city index...")
    city_urls = _get_city_urls()
    if not city_urls:
        print("[avvo] WARNING: Could not load city index")
        return []
    print(f"[avvo] Found {len(city_urls)} cities")

    if test_mode:
        city_urls = city_urls[:5]

    all_firms = []
    total_entries = 0

    for i, (city_name, city_url) in enumerate(city_urls):
        entries = _scrape_city(city_name, city_url, delay=delay)
        city_firms = _group_into_firms(entries, city_name)
        all_firms.extend(city_firms)
        total_entries += len(entries)

        if (i + 1) % 20 == 0:
            print(f"[avvo] Progress: {i+1}/{len(city_urls)} cities, "
                  f"{total_entries} attorneys, {len(all_firms)} entries")

    print(f"[avvo] Done: {len(all_firms)} entries from {len(city_urls)} cities")
    return all_firms


def merge_avvo_into_firms(firms: list, avvo_entries: list) -> list:
    """Merge Avvo data into existing firm list.

    Avvo entries are individual attorneys. We try to match them to existing
    firms, enriching with practice areas and phone. Unmatched entries are
    added as new firms.
    """
    merged = 0
    added = 0

    for avvo in avvo_entries:
        avvo_name = avvo["name"]
        avvo_city = avvo["address"]["city"].lower()
        matched = False

        for firm in firms:
            firm_city = (firm.get("address") or {}).get("city", "").lower()
            if firm_city != avvo_city:
                continue

            # Match by name similarity or phone
            same_name = are_same_firm(firm.get("name", ""), avvo_name, threshold=85)
            same_phone = (firm.get("phone") and avvo.get("phone")
                          and firm["phone"] == avvo["phone"])

            if same_name or same_phone:
                # Enrich existing firm with practice areas
                existing_pa = set(firm.get("practiceAreas") or [])
                for pa in avvo.get("practiceAreas", []):
                    if pa not in existing_pa:
                        firm.setdefault("practiceAreas", []).append(pa)
                        existing_pa.add(pa)
                if not firm.get("phone") and avvo.get("phone"):
                    firm["phone"] = avvo["phone"]
                if "avvo" not in firm.get("sources", []):
                    firm.setdefault("sources", []).append("avvo")
                matched = True
                merged += 1
                break

        if not matched:
            new_firm = {
                "id": str(uuid.uuid4()),
                "name": avvo_name,
                "practiceAreas": avvo.get("practiceAreas", []),
                "summary": None,
                "website": None,
                "phone": avvo.get("phone"),
                "email": None,
                "address": avvo.get("address", {"street": "", "city": "", "county": "",
                                                 "state": "KS", "zip": ""}),
                "coordinates": None,
                "referralScore": "low",
                "sources": ["avvo"],
            }
            firms.append(new_firm)
            added += 1

    print(f"[avvo] Merged into {merged} existing firms, added {added} new entries")
    return firms
