#!/usr/bin/env python3
"""Enrich existing firms with website URLs from multiple sources.

Phase 1: Crawl Justia Kansas lawyer directory — extract website from profiles
Phase 2: Search DuckDuckGo for firm-named entries — extract best non-directory result
Phase 3: Probe common domain patterns — HEAD request to check if URL resolves

Usage:
    python -m scraper.enrich_websites                  # Full run
    python -m scraper.enrich_websites --test            # Limit scope for testing
    python -m scraper.enrich_websites --skip-justia     # Skip Justia phase
    python -m scraper.enrich_websites --skip-search     # Skip DuckDuckGo phase
    python -m scraper.enrich_websites --skip-probing    # Skip domain probing phase
"""
import argparse
import json
import os
import re
import shutil
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, quote_plus, unquote

from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests
from dotenv import load_dotenv
import requests

from scraper.utils.normalize import normalize_firm_name, are_same_firm
from scraper.utils.enrich_cache import EnrichCache

# Load API keys from scraper/.env
load_dotenv("scraper/.env")

_IMPERSONATE = "chrome"
INPUT_PATH = "app/firms_data.js"
BACKUP_PATH = "/tmp/firms_data_enrich_websites_backup.js"

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; LawFirmDirectory/1.0)"}

# Domains to reject — directories, social media, not actual firm sites
DIRECTORY_DOMAINS = {
    # Major lawyer directories
    "findlaw.com", "lawyers.findlaw.com", "avvo.com", "justia.com",
    "lawyers.com", "martindale.com", "yelp.com", "yellowpages.com",
    "superlawyers.com", "nolo.com", "lawinfo.com", "hg.org",
    "lawyer.com", "bestlawyers.com", "usnews.com", "thumbtack.com",
    "lawyerguide.com", "lawyergist.com", "attorneyhelp.org",
    "attorneypages.com", "lawyerlegion.com", "topattorney.com",
    "alawyerin.us", "kansaslaw.com", "legal-companies.com",
    "ks.legal-companies.com", "dmlawusa.com", "dui.guru",
    "sage.law", "morelaw.com", "lawyerskc.net",
    # Social media / generic platforms
    "facebook.com", "linkedin.com", "twitter.com", "instagram.com",
    "bbb.org", "google.com", "bing.com", "mapquest.com", "manta.com",
    "chamberofcommerce.com", "hub.biz", "local.yahoo.com",
    # State/government directories
    "ksbar.org", "kscourts.org", "directory-kard.kscourts.gov",
    "kacdl.org",
    # Other directory / aggregator / review sites
    "superpages.com", "citysearch.com", "whitepages.com",
    "duckduckgo.com", "wikipedia.org", "youtube.com",
    "locaterecords.com", "repsight.com", "trustanalytica.org",
    "trellis.law",
    # Academic / legal reference (not firm sites)
    "cornell.edu", "law.cornell.edu", "lawyers.law.cornell.edu",
    "jstor.org", "uchicago.edu", "journals.uchicago.edu",
    "oclc.org", "contentdm.oclc.org", "kgi.contentdm.oclc.org",
    # News / archive / document hosting
    "lawrencekstimes.com", "newspaperarchive.com", "scribd.com",
    # Hosting/staging/CDN domains that aren't real firm sites
    "myftpupload.com", "wpengine.com", "wpenginepowered.com",
    "godaddy.com", "wixsite.com", "squarespace.com",
    "weebly.com", "wordpress.com",
}

# Overly generic domains that match many firms incorrectly
_GENERIC_DOMAINS = {"lawlaw.com", "law.com", "legal.com", "attorney.com", "lawyer.com"}

# Keywords that indicate a name is a firm (not a bare person name)
_FIRM_KEYWORDS = re.compile(
    r'\b(law|legal|office|firm|llc|llp|pllc|p\.?a\.?|pc|chartered|'
    r'group|associates?|counsel|advisors?|services|solutions|partners|'
    r'attorneys|lawyers)\b',
    re.IGNORECASE
)

# Parked domain indicators
_PARKED_PATTERNS = re.compile(
    r'(domain.{0,10}(for sale|is parked|expired|available)|'
    r'buy this domain|coming soon|under construction|'
    r'parked.{0,10}(free|domain)|godaddy\.com/forsale|'
    r'sedoparking|hugedomains|afternic|dan\.com)',
    re.IGNORECASE
)

# Suffixes to strip when generating domain candidates
_NAME_STRIP = re.compile(
    r'\b(llc|llp|pllc|p\.?a\.?|pc|inc|ltd|chartered|chtd|'
    r'attorney at law|attorneys at law|at law)\b',
    re.IGNORECASE
)


def _load_firms():
    with open(INPUT_PATH) as f:
        content = f.read()
    json_str = content[len("const FIRMS_DATA = "):-1]
    return json.loads(json_str)


_backup_done = False


def _save_firms(data):
    global _backup_done
    if not _backup_done:
        shutil.copy(INPUT_PATH, BACKUP_PATH)
        _backup_done = True
        print(f"[enrich-web] Original backed up to {BACKUP_PATH}")
    with open(INPUT_PATH, "w") as f:
        f.write("const FIRMS_DATA = ")
        json.dump(data, f, indent=2)
        f.write(";")
    print(f"[enrich-web] Saved to {INPUT_PATH}")


def _cffi_get(url, delay=1.0, timeout=10):
    """GET with curl_cffi browser impersonation for Cloudflare bypass."""
    try:
        r = cffi_requests.get(url, impersonate=_IMPERSONATE, timeout=timeout)
        time.sleep(delay)
        if r.status_code == 200:
            return BeautifulSoup(r.text, "lxml")
    except Exception:
        pass
    return None


def _build_firm_index(firms):
    """Build a city -> firms lookup for efficient matching."""
    by_city = {}
    for firm in firms:
        city = (firm.get("address") or {}).get("city", "").lower()
        if city:
            by_city.setdefault(city, []).append(firm)
    return by_city


def _is_directory_url(url):
    """Return True if URL belongs to a known directory (not a firm site)."""
    try:
        domain = urlparse(url).netloc.lower()
        # Strip www.
        if domain.startswith("www."):
            domain = domain[4:]
        # Check against known directories
        for d in DIRECTORY_DOMAINS:
            if domain == d or domain.endswith("." + d):
                return True
        # Check generic domains
        if domain in _GENERIC_DOMAINS:
            return True
    except Exception:
        return True
    return False


def _is_firm_like_name(name):
    """Return True if the name looks like a firm (not a bare person name)."""
    if _FIRM_KEYWORDS.search(name):
        return True
    # Names with comma likely have multiple people: "Smith, Jones & Brown"
    if "," in name or "&" in name:
        return True
    return False


def _validate_url(url, timeout=6):
    """HEAD-request a URL to check it resolves. Returns final URL or None."""
    try:
        r = requests.head(url, timeout=timeout, headers=HEADERS,
                          allow_redirects=True)
        if r.status_code < 400:
            final_url = r.url
            if _is_directory_url(final_url):
                return None
            return final_url
    except Exception:
        pass
    return None


def _check_not_parked(url, timeout=6):
    """GET a URL and check it's not a parked domain. Returns URL or None."""
    try:
        r = requests.get(url, timeout=timeout, headers=HEADERS,
                         allow_redirects=True)
        if r.status_code >= 400:
            return None
        if _is_directory_url(r.url):
            return None
        if _PARKED_PATTERNS.search(r.text[:5000]):
            return None
        return r.url, r.text
    except Exception:
        return None


def _validate_probed_url(url, firm_name, city, timeout=6):
    """Probe a URL and verify it belongs to a Kansas law firm.

    Returns the URL if valid, None otherwise.
    Checks:
    1. URL resolves and is not parked
    2. Page content mentions Kansas/KS or the firm's city
    3. Page appears to be a law firm (contains legal keywords)
    """
    result = _check_not_parked(url, timeout=timeout)
    if not result:
        return None
    final_url, html = result
    if not html:
        return None

    text = html[:15000].lower()

    # Must mention Kansas or the firm's city
    city_lower = city.lower() if city else ""
    has_location = (
        "kansas" in text
        or ", ks" in text
        or "ks " in text
        or (city_lower and city_lower in text)
    )
    if not has_location:
        return None

    # Should look like a law-related site
    has_legal = any(kw in text for kw in [
        "attorney", "lawyer", "law firm", "legal", "practice area",
        "consultation", "law office", "counsel",
    ])
    if not has_legal:
        return None

    return final_url


# ── Phase 0: Avvo Profile Crawl ────────────────────────────────────────────

_AVVO_BASE = "https://www.avvo.com"


def _get_avvo_city_urls(delay=1.0):
    """Fetch Avvo Kansas lawyer city index."""
    soup = _cffi_get(f"{_AVVO_BASE}/all-lawyers/ks.html", delay=delay)
    if not soup:
        return []
    seen = set()
    results = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/all-lawyers/ks/" in href and href != "/all-lawyers/ks.html":
            city = a.get_text(strip=True)
            url = href if href.startswith("http") else f"{_AVVO_BASE}{href}"
            if city and url not in seen:
                seen.add(url)
                results.append((city, url))
    return results


def _extract_avvo_profile_urls(soup):
    """Extract attorney profile URLs from an Avvo listing page."""
    seen = set()
    urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/attorneys/" in href:
            url = href if href.startswith("http") else f"{_AVVO_BASE}{href}"
            if url not in seen:
                seen.add(url)
                urls.append(url)
    return urls


def _extract_website_from_avvo_profile(soup, name=""):
    """Extract the attorney's website URL from their Avvo profile page."""
    # Look for "X's website" link pattern
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True).lower()
        href = a["href"]
        if ("website" in text and "avvo" not in text
                and href.startswith("http") and "avvo.com" not in href
                and "internetbrands.com" not in href):
            if not _is_directory_url(href):
                return href
    return None


def enrich_from_avvo(firms, delay=1.0, test_mode=False):
    """Phase 0: Crawl Avvo profiles to extract website URLs."""
    print("[avvo-enrich] Getting city index...")
    city_urls = _get_avvo_city_urls(delay=delay)
    if not city_urls:
        print("[avvo-enrich] Could not load city index — skipping")
        return 0

    if test_mode:
        city_urls = city_urls[:3]

    firm_index = _build_firm_index(firms)
    found = 0
    profiles_visited = 0
    cities_done = 0

    print(f"[avvo-enrich] Crawling {len(city_urls)} cities for profiles...")

    for city_name, city_url in city_urls:
        city_key = city_name.lower().strip()
        city_firms = firm_index.get(city_key, [])
        # Only bother if we have firms without websites in this city
        needs_website = [f for f in city_firms if not f.get("website")]
        if not needs_website:
            cities_done += 1
            continue

        # Crawl listing pages to get profile URLs
        url = city_url
        page = 1
        profile_urls = []

        while url and page <= 20:
            soup = _cffi_get(url, delay=delay)
            if not soup:
                break

            new_profiles = _extract_avvo_profile_urls(soup)
            if not new_profiles and page > 1:
                break
            profile_urls.extend(new_profiles)

            # Pagination
            page += 1
            next_url = re.sub(r"\?page=\d+", "", city_url.rstrip("/"))
            next_url = f"{next_url}?page={page}"
            # Check if there's an actual next page link
            page_links = soup.find_all("a", href=lambda h: h and "page=" in str(h))
            max_page = 1
            for pl in page_links:
                try:
                    num = int(pl.get_text(strip=True))
                    max_page = max(max_page, num)
                except ValueError:
                    pass
            if page <= max_page:
                url = next_url
            else:
                break

        # Visit each profile and try to extract website
        for prof_url in profile_urls:
            soup = _cffi_get(prof_url, delay=delay, timeout=10)
            if not soup:
                continue
            profiles_visited += 1

            website = _extract_website_from_avvo_profile(soup)
            if not website:
                continue

            # Get attorney name from profile page
            name_el = soup.find("h1") or soup.find("span", class_=re.compile(r"name", re.I))
            prof_name = name_el.get_text(strip=True) if name_el else ""

            # Try to match to a firm without a website
            for firm in needs_website:
                if firm.get("website"):
                    continue
                if are_same_firm(firm["name"], prof_name, threshold=80):
                    firm["website"] = website
                    if "avvo" not in (firm.get("sources") or []):
                        firm.setdefault("sources", []).append("avvo")
                    found += 1
                    break

        cities_done += 1
        if cities_done % 10 == 0:
            print(f"[avvo-enrich] {cities_done}/{len(city_urls)} cities, "
                  f"{profiles_visited} profiles, +{found} websites")

    print(f"[avvo-enrich] Done: {cities_done} cities, {profiles_visited} profiles, "
          f"+{found} websites")
    return found


# ── Phase 1: Justia Directory Crawl ────────────────────────────────────────

def _get_justia_city_urls(delay=1.0):
    """Fetch Justia Kansas lawyers index and return city URLs."""
    soup = _cffi_get("https://www.justia.com/lawyers/kansas", delay=delay)
    if not soup:
        return []
    results = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Look for city links like /lawyers/kansas/wichita
        if re.search(r"/lawyers/kansas/[\w-]+", href):
            text = a.get_text(strip=True)
            if text and href not in seen:
                # Normalize URL
                if not href.startswith("http"):
                    href = f"https://www.justia.com{href}"
                seen.add(href)
                results.append((text, href))
    return results


def _extract_justia_profiles(soup):
    """Extract lawyer profiles from a Justia listing page."""
    results = []

    # Justia uses various card structures — try multiple selectors
    # Look for lawyer profile links with website info
    for card in soup.find_all(["div", "li"], class_=lambda c: c and any(
            x in str(c).lower() for x in ["lawyer", "profile", "listing", "result", "attorney"])):
        name = None
        website = None

        # Find the lawyer name (usually in a heading or strong link)
        for heading in card.find_all(["h2", "h3", "h4", "a", "strong"]):
            text = heading.get_text(strip=True)
            if text and len(text) > 3 and not re.match(r'^\d+$', text):
                # Skip if it's a practice area or generic text
                if any(skip in text.lower() for skip in ["practice area", "show more",
                                                          "view profile", "page", "next"]):
                    continue
                name = text
                break

        if not name:
            continue

        # Find external website link
        for a in card.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True).lower()
            # Look for explicit "website" links
            if ("website" in text or "visit" in text) and href.startswith("http"):
                if not _is_directory_url(href):
                    website = re.sub(r"\?.*$", "", href)
                    break
            # Also check for links that go to external domains (not justia)
            if href.startswith("http") and "justia.com" not in href:
                if not _is_directory_url(href) and re.search(
                        r"\.(com|law|legal|net|org|us|attorney|lawyer)(/|$)", href):
                    website = re.sub(r"\?.*$", "", href)

        if website:
            results.append({"name": name, "website": website})

    # Broader fallback: look for any external links paired with names
    if not results:
        # Try finding profile sections more generically
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True).lower()
            if "website" in text and href.startswith("http") and "justia.com" not in href:
                if not _is_directory_url(href):
                    # Try to find the name near this link
                    parent = a.find_parent(["div", "li", "section", "article"])
                    if parent:
                        for heading in parent.find_all(["h2", "h3", "h4", "strong", "a"]):
                            name_text = heading.get_text(strip=True)
                            if (name_text and len(name_text) > 3
                                    and "website" not in name_text.lower()
                                    and not re.match(r'^\d+$', name_text)):
                                results.append({
                                    "name": name_text,
                                    "website": re.sub(r"\?.*$", "", href),
                                })
                                break

    return results


def enrich_from_justia(firms, delay=1.0, test_mode=False):
    """Phase 1: Crawl Justia Kansas directory city-by-city."""
    print("[justia] Getting city index...")
    city_urls = _get_justia_city_urls(delay=delay)
    if not city_urls:
        print("[justia] Could not load city index — skipping")
        return 0

    if test_mode:
        city_urls = city_urls[:5]

    firm_index = _build_firm_index(firms)
    found = 0
    cities_done = 0
    pages = 0

    print(f"[justia] Crawling {len(city_urls)} cities...")

    for city_name, city_url in city_urls:
        city_key = city_name.lower().strip()
        url = city_url
        page = 1
        city_pages = 0

        while url and page <= 5:
            soup = _cffi_get(url, delay=delay)
            if not soup:
                break

            profiles = _extract_justia_profiles(soup)
            city_pages += 1
            for prof in profiles:
                if not prof.get("website"):
                    continue
                # Try to match against firms in this city
                candidates = firm_index.get(city_key, [])
                for firm in candidates:
                    if firm.get("website"):
                        continue
                    if are_same_firm(firm["name"], prof["name"], threshold=80):
                        firm["website"] = prof["website"]
                        if "justia" not in (firm.get("sources") or []):
                            firm.setdefault("sources", []).append("justia")
                        found += 1
                        break

            pages += 1

            # Pagination — look for "Next" link
            next_link = soup.find("a", string=re.compile(r"Next|›|»", re.I), href=True)
            if next_link:
                href = next_link["href"]
                if not href.startswith("http"):
                    href = f"https://www.justia.com{href}"
                url = href
                page += 1
            else:
                break

        cities_done += 1
        print(f"[justia] {city_name}: {city_pages} pages, +{found} total websites so far")
        if cities_done % 10 == 0:
            print(f"[justia] === {cities_done}/{len(city_urls)} cities done, "
                  f"{pages} total pages ===")

    print(f"[justia] Done: {cities_done} cities, {pages} pages, +{found} websites")
    return found


# ── Phase 2: Web Search ────────────────────────────────────────────────────

_BRAVE_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"


def _brave_search(query, delay=0.5, count=10):
    """Query Brave Search API and return result URLs.

    Requires BRAVE_API_KEY in scraper/.env. Returns [] if no key or on error.
    Brave pricing: ~$3/1k queries on the Pro tier; 2k/mo free tier available.
    """
    api_key = os.getenv("BRAVE_API_KEY") or os.getenv("BRAVE_SEARCH_API_KEY")
    if not api_key:
        return []
    try:
        r = requests.get(
            _BRAVE_ENDPOINT,
            params={"q": query, "count": count, "country": "US"},
            headers={
                "X-Subscription-Token": api_key,
                "Accept": "application/json",
            },
            timeout=15,
        )
        time.sleep(delay)  # Brave Pro allows ~20 req/s; free tier is 1 req/s
        if r.status_code != 200:
            return []
        payload = r.json()
    except Exception:
        return []

    web = (payload.get("web") or {}).get("results") or []
    return [item.get("url") for item in web if item.get("url")]


def _duckduckgo_search(query, delay=2.5):
    """Search DuckDuckGo HTML version and return result URLs."""
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
    try:
        r = cffi_requests.get(url, impersonate=_IMPERSONATE, timeout=15,
                              headers={"Accept-Language": "en-US,en;q=0.9"})
        time.sleep(delay)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "lxml")
    except Exception:
        return []

    results = []
    for a in soup.find_all("a", class_="result__a", href=True):
        href = a["href"]
        if "uddg=" in href:
            m = re.search(r"uddg=([^&]+)", href)
            if m:
                actual_url = unquote(m.group(1))
                if actual_url.startswith("http"):
                    results.append(actual_url)
        elif href.startswith("http") and "duckduckgo.com" not in href:
            results.append(href)

    if not results:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "uddg=" in href:
                m = re.search(r"uddg=([^&]+)", href)
                if m:
                    actual_url = unquote(m.group(1))
                    if actual_url.startswith("http"):
                        results.append(actual_url)

    return results[:15]


def _google_search(query, delay=3.0):
    """Search Google and return result URLs using curl_cffi for Cloudflare bypass."""
    url = f"https://www.google.com/search?q={quote_plus(query)}&num=10&hl=en"
    try:
        r = cffi_requests.get(url, impersonate=_IMPERSONATE, timeout=15,
                              headers={"Accept-Language": "en-US,en;q=0.9"})
        time.sleep(delay)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "lxml")
    except Exception:
        return []

    results = []
    # Google organic results are in <a> tags with href starting with /url?q=
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/url?q="):
            m = re.search(r"/url\?q=([^&]+)", href)
            if m:
                actual_url = unquote(m.group(1))
                if actual_url.startswith("http") and "google.com" not in actual_url:
                    results.append(actual_url)

    # Fallback: look for direct links in result divs
    if not results:
        for a in soup.select("a[data-ved]"):
            href = a.get("href", "")
            if href.startswith("http") and "google.com" not in href:
                results.append(href)

    return results[:15]


def _bing_search(query, delay=2.0):
    """Search Bing and return result URLs."""
    url = f"https://www.bing.com/search?q={quote_plus(query)}"
    try:
        r = cffi_requests.get(url, impersonate=_IMPERSONATE, timeout=15,
                              headers={"Accept-Language": "en-US,en;q=0.9"})
        time.sleep(delay)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "lxml")
    except Exception:
        return []

    results = []
    # Bing organic results have <li class="b_algo"> with <a href="...">
    for li in soup.select("li.b_algo"):
        a = li.find("a", href=True)
        if a:
            href = a["href"]
            if href.startswith("http") and "bing.com" not in href:
                results.append(href)

    # Fallback: any <cite> tags often contain the URL
    if not results:
        for cite in soup.find_all("cite"):
            text = cite.get_text(strip=True)
            if text.startswith("http"):
                results.append(text)
            elif "." in text and "/" not in text[:20]:
                results.append(f"https://{text}")

    return results[:15]


def _web_search(query, delay=3.0):
    """Try multiple search engines, return first successful result set.

    Prefer Brave Search API when BRAVE_API_KEY is configured (reliable,
    no rate-limit roulette). Fall back to scraped engines otherwise.
    """
    # Brave first when key is available — cheapest + most reliable
    urls = _brave_search(query, delay=0.5)
    if urls:
        return urls, "brave"

    urls = _google_search(query, delay=delay)
    if urls:
        return urls, "google"

    urls = _bing_search(query, delay=delay)
    if urls:
        return urls, "bing"

    urls = _duckduckgo_search(query, delay=delay)
    if urls:
        return urls, "ddg"

    return [], None


def _pick_best_result(urls, firm_name):
    """From search results, pick the most likely firm website URL."""
    norm_name = normalize_firm_name(firm_name).lower()
    # Extract key words from the firm name for domain matching
    name_words = [w for w in norm_name.split() if len(w) > 2]

    candidates = []
    for url in urls:
        if _is_directory_url(url):
            continue
        try:
            domain = urlparse(url).netloc.lower()
            if domain.startswith("www."):
                domain = domain[4:]
        except Exception:
            continue

        # Score the URL
        score = 0
        domain_base = domain.split(".")[0]

        # Domain contains part of the firm name
        for word in name_words:
            if word in domain_base:
                score += 3

        # Preferred TLDs for law firms
        if any(domain.endswith(tld) for tld in [".law", ".legal", ".attorney", ".lawyer"]):
            score += 2
        elif domain.endswith(".com"):
            score += 1

        # "law" in domain is a strong signal
        if "law" in domain_base:
            score += 2

        candidates.append((score, url))

    if not candidates:
        return None

    # Sort by score descending, return the best
    candidates.sort(key=lambda x: -x[0])
    best_score, best_url = candidates[0]

    # Accuracy bar: require a meaningful name/domain match. Without it,
    # falling back to "first result" has produced too many false positives
    # (aggregator sites, competitor firms, unrelated businesses).
    if best_score >= 2:
        return best_url
    return None


def enrich_from_search(firms, delay=2.5, test_mode=False, force=False):
    """Phase 2: Search engines for firm websites.

    Uses EnrichCache to skip firms that were recently searched (hit or fresh miss).
    Pass force=True to re-search everything.
    """
    # Only search for firm-like names — bare person names produce too many false positives
    candidates = [f for f in firms if not f.get("website") and _is_firm_like_name(f["name"])]

    if test_mode:
        candidates = candidates[:30]

    cache = EnrichCache()
    pre_skip = sum(1 for f in candidates if cache.should_skip(f.get("id"), force=force))
    print(f"[search] {len(candidates)} firm-named entries to search "
          f"({pre_skip} will be skipped by cache)")

    found = 0
    searched = 0
    cache_skipped = 0
    consecutive_failures = 0

    for i, firm in enumerate(candidates):
        if firm.get("website"):  # May have been set by earlier match
            continue

        if cache.should_skip(firm.get("id"), force=force):
            cache_skipped += 1
            continue

        city = (firm.get("address") or {}).get("city", "")
        query = f'"{firm["name"]}" {city} KS attorney'

        urls, engine = _web_search(query, delay=delay)

        if not urls:
            cache.record(firm.get("id"), query, None, None)
            consecutive_failures += 1
            if consecutive_failures >= 15:
                delay = min(delay + 0.5, 8.0)
                print(f"[search] Increasing delay to {delay}s after {consecutive_failures} failures")
            if consecutive_failures >= 50:
                print("[search] Too many consecutive failures — aborting search phase")
                break
            searched += 1
            continue

        consecutive_failures = 0
        best = _pick_best_result(urls, firm["name"])

        validated = None
        if best:
            validated = _validate_url(best)
            if validated and not _is_directory_url(validated):
                firm["website"] = validated
                found += 1
            else:
                validated = None

        cache.record(firm.get("id"), query, validated, engine)
        searched += 1

        if searched % 25 == 0:
            cache.save()  # persist incrementally
            print(f"[search] Progress: {searched}/{len(candidates)}, "
                  f"+{found} websites ({engine}), cache_skip={cache_skipped}")

    cache.save()
    print(f"[search] Done: {searched} searched, +{found} websites, {cache_skipped} cache-skipped")
    return found


# ── Phase 3: Direct URL Probing ────────────────────────────────────────────

def _generate_domain_candidates(firm_name):
    """Generate plausible domain names from a firm name."""
    # Clean the name
    name = firm_name.lower()
    name = _NAME_STRIP.sub("", name)
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()

    words = name.split()
    if not words:
        return []

    # Core name (first significant word, usually a surname)
    core = words[0]
    # Full name joined
    full_joined = "".join(words)
    full_dashed = "-".join(words)

    domains = []

    # Common patterns
    if len(words) > 1:
        domains.append(f"https://www.{full_joined}.com")
        domains.append(f"https://www.{full_dashed}.com")

    domains.append(f"https://www.{core}law.com")
    domains.append(f"https://www.{core}lawfirm.com")
    domains.append(f"https://www.{core}lawoffice.com")
    domains.append(f"https://www.{core}.law")

    if len(words) > 1:
        # e.g. "smithjones" for "Smith Jones"
        two = words[0] + words[1] if len(words) > 1 else words[0]
        domains.append(f"https://www.{two}law.com")
        domains.append(f"https://www.{two}.com")

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for d in domains:
        if d not in seen:
            seen.add(d)
            unique.append(d)

    return unique


def enrich_from_domain_probing(firms, delay=0.5, test_mode=False):
    """Phase 3: Try common domain patterns via HEAD requests."""
    candidates = [f for f in firms if not f.get("website") and _is_firm_like_name(f["name"])]

    if test_mode:
        candidates = candidates[:30]

    print(f"[probe] {len(candidates)} firms to probe...")

    found = 0
    probed = 0

    for firm in candidates:
        if firm.get("website"):
            continue

        domains = _generate_domain_candidates(firm["name"])

        city = (firm.get("address") or {}).get("city", "")
        for url in domains:
            time.sleep(delay)
            validated = _validate_probed_url(url, firm["name"], city, timeout=4)
            if validated and not _is_directory_url(validated):
                firm["website"] = validated
                found += 1
                break

        probed += 1

        if probed % 100 == 0:
            print(f"[probe] Progress: {probed}/{len(candidates)}, +{found} websites")

    print(f"[probe] Done: {probed} probed, +{found} websites")
    return found


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Enrich firm data with website URLs")
    parser.add_argument("--skip-avvo", action="store_true")
    parser.add_argument("--skip-justia", action="store_true")
    parser.add_argument("--skip-search", action="store_true")
    parser.add_argument("--skip-probing", action="store_true")
    parser.add_argument("--only-avvo", action="store_true", help="Run only the Avvo phase")
    parser.add_argument("--test", action="store_true", help="Limit scope for testing")
    parser.add_argument("--delay", type=float, default=1.0)
    parser.add_argument("--force", action="store_true",
                        help="Ignore search cache and re-query every firm")
    args = parser.parse_args()

    data = _load_firms()
    firms = data["firms"]

    before = sum(1 for f in firms if f.get("website"))
    print(f"[enrich-web] Starting: {len(firms)} firms, {before} with websites, "
          f"{len(firms) - before} missing")

    # Phase 0: Avvo profile crawl (highest yield — visits profiles for website links)
    if not args.skip_avvo:
        enrich_from_avvo(firms, delay=args.delay, test_mode=args.test)
        data["firms"] = firms
        _save_firms(data)

    if args.only_avvo:
        after = sum(1 for f in firms if f.get("website"))
        print(f"\n{'='*50}")
        print(f"  Website Enrichment Complete (Avvo only)")
        print(f"{'='*50}")
        print(f"  Websites: {before} → {after} (+{after - before})")
        print(f"  Coverage: {before*100//len(firms)}% → {after*100//len(firms)}%")
        print(f"{'='*50}")
        return

    # Phase 1: Justia
    if not args.skip_justia:
        enrich_from_justia(firms, delay=args.delay, test_mode=args.test)
        data["firms"] = firms
        _save_firms(data)

    # Phase 2: Web search (Brave / Google / Bing / DuckDuckGo)
    if not args.skip_search:
        enrich_from_search(firms, delay=max(args.delay, 2.5),
                           test_mode=args.test, force=args.force)
        data["firms"] = firms
        _save_firms(data)

    # Phase 3: Domain probing
    if not args.skip_probing:
        enrich_from_domain_probing(firms, delay=0.5, test_mode=args.test)
        data["firms"] = firms

    # Final save
    data["meta"]["lastScraped"] = datetime.now(timezone.utc).isoformat()
    _save_firms(data)

    after = sum(1 for f in firms if f.get("website"))
    print(f"\n{'='*50}")
    print(f"  Website Enrichment Complete")
    print(f"{'='*50}")
    print(f"  Websites: {before} → {after} (+{after - before})")
    print(f"  Coverage: {before*100//len(firms)}% → {after*100//len(firms)}%")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
