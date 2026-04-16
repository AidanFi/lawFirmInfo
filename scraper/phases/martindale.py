"""Martindale-Hubbell directory scraper (enrichment mode).

Crawls https://www.martindale.com/all-lawyers/{city}/kansas/ and extracts
attorney/firm info directly from listing cards. Each card includes:
  - Attorney name
  - Firm name (link to /organization/)
  - City, state
  - Phone (tel: link)
  - Website (external http:// link)

Profile pages are rate-limited (429), so we only scrape listing pages.
Also handles pagination (?page=2, ?page=3...).

Matches back to firm list by fuzzy name/city comparison.
"""
import re
import time
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests

from scraper.utils.normalize import are_same_firm, normalize_firm_name

_BASE = "https://www.martindale.com"
_IMPERSONATE = "chrome"

# Reuse directory blocklist from enrich_websites
from scraper.enrich_websites import _is_directory_url  # noqa: E402


def _get(url, delay=1.5, timeout=20):
    """GET with curl_cffi browser impersonation."""
    try:
        r = cffi_requests.get(url, impersonate=_IMPERSONATE, timeout=timeout)
        time.sleep(delay)
        if r.status_code == 200:
            return BeautifulSoup(r.text, "lxml")
        if r.status_code == 429:
            # Backoff
            time.sleep(delay * 5)
    except Exception:
        pass
    return None


def _extract_city_urls(soup):
    """Extract city listing URLs from the Kansas index page."""
    urls = []
    seen = set()
    for a in soup.find_all("a", href=True):
        h = a["href"]
        if "/all-lawyers/" in h and "/kansas" in h:
            full = h if h.startswith("http") else f"{_BASE}{h}"
            if full not in seen:
                seen.add(full)
                city = a.get_text(strip=True)
                urls.append((city, full))
    return urls


def _extract_listings(soup):
    """Extract attorney+firm entries from a city listing page.

    Each entry = dict with attorney_name, firm_name, phone, website, city.
    """
    entries = []
    # Cards have class 'card--attorney'
    cards = soup.find_all(attrs={"class": re.compile(r"card--attorney|card attorney", re.I)})
    # Filter to ones with a profile link (avoid sidebar blocks)
    cards = [c for c in cards if c.find("a", href=re.compile(r"/attorney/[\w-]+-\d+/?"))]

    for card in cards:
        data = {"attorney_name": None, "firm_name": None, "website": None,
                "phone": None, "city": None}

        # Attorney name: first link to /attorney/ with non-empty text
        for a in card.find_all("a", href=re.compile(r"/attorney/[\w-]+-\d+/?")):
            t = a.get_text(strip=True)
            if t and not t.startswith("("):
                data["attorney_name"] = t
                break

        # Firm name: link to /organization/
        for a in card.find_all("a", href=True):
            h = a["href"]
            if "/organization/" in h or "/law-firm/" in h or "/firm/" in h:
                name = a.get_text(strip=True)
                if name and len(name) > 2:
                    data["firm_name"] = name
                    break

        # Phone: first tel: link
        for a in card.find_all("a", href=True):
            h = a["href"]
            if h.startswith("tel:"):
                data["phone"] = h.replace("tel:", "").strip()
                break

        # Website: external http(s) link not on martindale/lawyers.com
        for a in card.find_all("a", href=True):
            h = a["href"]
            if (h.startswith("http")
                    and "martindale.com" not in h
                    and "lawyers.com" not in h
                    and not h.startswith("tel:")):
                # Skip social/video-call links
                host = urlparse(h).netloc.lower()
                if any(b in host for b in (
                    "facebook.com", "twitter.com", "linkedin.com",
                    "youtube.com", "instagram.com", "zoom.us",
                    "googleusercontent.com",
                )):
                    continue
                if _is_directory_url(h):
                    continue
                data["website"] = h
                break

        # City from text — "Wichita, KS"
        txt = card.get_text(separator="|", strip=True)
        m = re.search(r"([A-Z][a-zA-Z \-]+),\s*KS\b", txt)
        if m:
            data["city"] = m.group(1).strip()

        if data["attorney_name"] or data["firm_name"]:
            entries.append(data)

    return entries


def _extract_next_page_url(soup, current_url):
    """Find next-page link on a listing page."""
    # Look for rel=next or ?page=N links
    for a in soup.find_all("a", href=True):
        rel = a.get("rel") or []
        if "next" in rel:
            h = a["href"]
            return h if h.startswith("http") else f"{_BASE}{h}"
    # Fallback: look for "Next" text
    for a in soup.find_all("a", href=True):
        t = a.get_text(strip=True).lower()
        if t in ("next", "next »", "»"):
            h = a["href"]
            return h if h.startswith("http") else f"{_BASE}{h}"
    return None


_FIRM_TOKENS = (" llc", " llp", " l.l.p", " l.l.c", " pa", " p.a.",
                " law", " firm", " office", " associates", " group",
                " & ", " chartered", " attorneys", " partners", " pllc",
                " lc", " l.c.")


def _looks_like_firm(name: str) -> bool:
    """Return True if name has firm-indicator tokens."""
    if not name:
        return False
    return any(t in name.lower() for t in _FIRM_TOKENS)


def scrape_martindale(firms_index, cities=None, delay=1.5, max_pages_per_city=5,
                      test_mode=False, add_new=True):
    """Enrich *firms_index* with websites from Martindale listings.

    Args:
        firms_index: list of firm dicts (mutated in place)
        cities: optional list of cities to restrict to; default = all KS cities
        delay: seconds between HTTP requests
        max_pages_per_city: pagination cap
        test_mode: only process 3 cities
        add_new: if True, add net-new firms that don't match anything

    Returns (added_websites, new_firms_count).
    """
    import uuid
    from collections import defaultdict

    # Firms missing websites, indexed by city
    by_city = defaultdict(list)
    for firm in firms_index:
        if firm.get("website"):
            continue
        c = ((firm.get("address") or {}).get("city") or "").lower()
        if c:
            by_city[c].append(firm)

    # ALL firms, for dedup check when adding net-new
    all_firms_by_city = defaultdict(list)
    for firm in firms_index:
        c = ((firm.get("address") or {}).get("city") or "").lower()
        if c:
            all_firms_by_city[c].append(firm)
    # Also index by normalized name for cross-city dedup
    all_names_seen = {normalize_firm_name(f["name"]): f for f in firms_index if f.get("name")}

    print("[martindale] Getting Kansas city index...")
    soup = _get(f"{_BASE}/by-location/kansas-lawyers/", delay=delay)
    if not soup:
        print("[martindale] Could not load city index — skipping")
        return 0, 0

    city_urls = _extract_city_urls(soup)
    print(f"[martindale] Found {len(city_urls)} city pages")

    if cities:
        want = {c.lower() for c in cities}
        city_urls = [(n, u) for n, u in city_urls if n.lower() in want]
        print(f"[martindale] Filtered to {len(city_urls)} cities")

    if test_mode:
        city_urls = city_urls[:3]

    added_websites = 0
    new_firms_added = 0
    entries_seen = 0
    cities_done = 0

    for city_name, city_url in city_urls:
        city_key = city_name.lower().strip()
        city_firms_needing = by_city.get(city_key, [])
        city_firms_all = all_firms_by_city.get(city_key, [])
        if not city_firms_needing and not test_mode and not add_new:
            cities_done += 1
            continue

        page_url = city_url
        city_added = 0
        city_entries = 0

        for page_i in range(max_pages_per_city):
            soup = _get(page_url, delay=delay)
            if not soup:
                break

            entries = _extract_listings(soup)
            if not entries:
                break
            city_entries += len(entries)
            entries_seen += len(entries)

            for entry in entries:
                if not entry.get("website"):
                    continue

                match_name = entry.get("firm_name") or entry.get("attorney_name")
                if not match_name:
                    continue

                matched = False
                for firm in city_firms_needing:
                    if firm.get("website"):
                        continue
                    if are_same_firm(firm["name"], match_name, threshold=80):
                        firm["website"] = entry["website"]
                        if "martindale" not in (firm.get("sources") or []):
                            firm.setdefault("sources", []).append("martindale")
                        added_websites += 1
                        city_added += 1
                        matched = True
                        break

                # Also try attorney name
                if not matched and entry.get("attorney_name"):
                    for firm in city_firms_needing:
                        if firm.get("website"):
                            continue
                        if are_same_firm(firm["name"], entry["attorney_name"], threshold=85):
                            firm["website"] = entry["website"]
                            if "martindale" not in (firm.get("sources") or []):
                                firm.setdefault("sources", []).append("martindale")
                            added_websites += 1
                            city_added += 1
                            matched = True
                            break

                # Net-new firm: only add if firm_name looks like a firm
                # and we haven't seen anything close to this name yet
                if not matched and add_new and entry.get("firm_name"):
                    fn = entry["firm_name"]
                    if not _looks_like_firm(fn):
                        continue
                    # Check against all firms in this city (including those with websites)
                    already_exists = False
                    for f in city_firms_all:
                        if are_same_firm(f["name"], fn, threshold=80):
                            already_exists = True
                            break
                    if already_exists:
                        continue
                    # Cross-city check by normalized name
                    norm = normalize_firm_name(fn)
                    if norm in all_names_seen:
                        continue

                    new_firm = {
                        "id": f"martindale-{uuid.uuid4().hex[:8]}",
                        "name": fn,
                        "address": {"city": city_name, "state": "KS"},
                        "website": entry["website"],
                        "sources": ["martindale"],
                    }
                    if entry.get("phone"):
                        new_firm["phone"] = entry["phone"]
                    if entry.get("attorney_name"):
                        new_firm["attorneys"] = [{"name": entry["attorney_name"]}]
                    firms_index.append(new_firm)
                    all_names_seen[norm] = new_firm
                    city_firms_all.append(new_firm)
                    new_firms_added += 1
                    city_added += 1

            # Next page
            next_url = _extract_next_page_url(soup, page_url)
            if not next_url or next_url == page_url:
                break
            page_url = next_url

        cities_done += 1
        if city_added or cities_done % 10 == 0:
            print(f"[martindale] {city_name}: {city_added} new websites "
                  f"({city_entries} entries, {cities_done}/{len(city_urls)} cities)")

    print(f"[martindale] Done: {cities_done} cities, {entries_seen} entries seen, "
          f"+{added_websites} websites, +{new_firms_added} new firms")
    return added_websites, new_firms_added
