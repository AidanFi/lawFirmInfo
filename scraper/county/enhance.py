import html
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
    # Medical/Health
    "hospital", "medical center", "pharmacy", "dental", "dentist",
    "chiropractic", "physical therapy", "health club", "clinic",
    "dds", "orthodont", "veterinar",
    # Food/Hospitality
    "restaurant", "pub ", "patio", "cafe", "coffee",
    # Home services
    "cleaning", "plumbing", "roofing", "heating", "cooling", "hvac",
    "construction", "painting", "pest control",
    # Lawn/Garden
    "lawn", "landscap", "mowing", "irrigation", "tree care",
    "garden center", "garden farm", "lawncare",
    # Real estate/Finance
    "realty", "real estate group", "mortgage", "credit union",
    "financial group", "investments", "financial partners", "advisors",
    "insurance", "title loans",
    # Specific companies
    "gas station", "mattress", "salon", " spa",
    "t-mobile", "activision", "blizzard", "morgan stanley",
    "tyler technologies", "ikea", "scheels", "h&r block",
    "wells fargo", "raymond james", "lendnation", "united parcel",
    "rehrig", "fedex",
    # Government/Education/Civic
    "school district", "university", "college",
    "county government", "city of ", "state of ", "city hall",
    # Parks/Recreation
    "dog park", "skate park", "memorial park", "arboretum",
    "office park", "soccer complex", "sanctuary", " mall",
    # Media
    "gazette", "broadcasting",
    # Other non-legal
    "snow removal", "feed ", "consulting services",
    "manufacturing", "service center", "training solutions",
    "business solutions", "smiles", "law enforcement",
    "air conditioning", "refrigerator", "watercooler", "enterprises",
)

_LEGAL_RE = re.compile(
    r'\b(?:'
    r'law\s+(?:firm|office|offices|group|center|practice)|'
    r'legal|attorney|lawyer|counsel|advocate|esquire|esq\.?|'
    r'bankruptcy'
    r')\b',
    re.IGNORECASE,
)

_LEGAL_SUFFIX_RE = re.compile(
    r'\b(?:llc|llp|pllc|p\.?a\.?|p\.?c\.?|chartered)\b',
    re.IGNORECASE,
)

_TRUSTED_LEGAL_SOURCES = frozenset({
    "google_places", "foursquare", "ks_courts", "martindale",
})

_NONLEGAL_RE = re.compile(
    r'\bpark\s*$'
    r'|\bbank\b(?!rupt)',
    re.IGNORECASE,
)


def _looks_like_legal_entity(
    name: str, practice_areas: list, sources: list | None = None,
) -> bool:
    lower = name.lower()
    if _LEGAL_RE.search(lower):
        return True
    # Check non-legal BEFORE legal suffix — catches "DDS PA" dental offices
    if any(ind in lower for ind in _NON_LEGAL_INDICATORS):
        return False
    if _NONLEGAL_RE.search(lower):
        return False
    if _LEGAL_SUFFIX_RE.search(lower):
        return True
    if sources and _TRUSTED_LEGAL_SOURCES & set(sources):
        return True
    if practice_areas:
        return True
    return False


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


_OUT_OF_STATE_RE = re.compile(
    r'\b(?:miami|arizona|california|colorado|shreveport|suncoast|'
    r'sarasota|st\.?\s*louis|chicago|houston|dallas|los angeles|'
    r'new york|atlanta|detroit|phoenix|denver)\b',
    re.IGNORECASE,
)

_GARBAGE_RE = re.compile(
    r'\b(?:seo|sitemap|residents|subscribe|click here|read more|'
    r'homepage|sign up|download|log ?in)\b',
    re.IGNORECASE,
)

_PO_BOX_RE = re.compile(r'(?:p\.?o\.?\s*box|pmb)\b', re.IGNORECASE)


def _sanitize_firm_name(name: str) -> str | None:
    name = html.unescape(name).strip()
    if not name:
        return None
    if "http://" in name or "https://" in name or "www." in name:
        return None
    if ".com/" in name or ".org/" in name or ".net/" in name:
        return None
    idx = name.find("...")
    if idx != -1:
        name = name[:idx].strip()
        if len(name) < 5:
            return None
    if ": " in name:
        name = name.split(": ")[0].strip()
    if len(name) > 80:
        return None
    if _GARBAGE_RE.search(name):
        return None
    if _OUT_OF_STATE_RE.search(name):
        return None
    if "<" in name or ">" in name:
        return None
    return name or None


def _sanitize_address(addr: dict) -> dict:
    street = addr.get("street", "")
    if not street:
        return addr
    if len(street) > 120:
        addr["street"] = ""
        return addr
    if not _PO_BOX_RE.search(street) and not re.search(r'\d', street):
        addr["street"] = ""
        return addr
    return addr


# ---------------------------------------------------------------------------
# Sub-step 1: KS Courts full scraper (cached)
# ---------------------------------------------------------------------------

_KS_COURTS_CACHE_MAX_AGE_DAYS = 30


def _load_ks_courts_cache(cache_path: str) -> list | None:
    if not os.path.exists(cache_path):
        return None
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        from datetime import datetime, timezone
        cached_at = datetime.fromisoformat(data["cached_at"])
        age_days = (datetime.now(timezone.utc) - cached_at).days
        if age_days > _KS_COURTS_CACHE_MAX_AGE_DAYS:
            print(f"  [enhance] KS Courts cache expired ({age_days} days old)")
            return None
        print(f"  [enhance] KS Courts cache loaded ({len(data['firms'])} firms, {age_days} days old)")
        return data["firms"]
    except Exception as e:
        print(f"  [enhance] KS Courts cache read failed: {e}")
        return None


def _save_ks_courts_cache(firms: list, cache_path: str):
    from datetime import datetime, timezone
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump({"cached_at": datetime.now(timezone.utc).isoformat(), "firms": firms}, f)
    print(f"  [enhance] KS Courts cache saved ({len(firms)} firms)")


def _enrich_from_ks_courts(firms: list, county_config: dict, test_mode: bool = False) -> int:
    slug = county_config["slug"]
    cache_path = os.path.join("data", "county", f"{slug}_ks_courts_cache.json")

    ks_firms = _load_ks_courts_cache(cache_path)
    if ks_firms is None:
        try:
            from scraper.phases.ks_courts import scrape_ks_courts
        except ImportError:
            print("  [enhance] ks_courts module not available — skipping")
            return 0

        print("  [enhance] Running full KS Courts scraper (this may take a while)...")
        try:
            ks_firms_raw = scrape_ks_courts(test_mode=test_mode)
            ks_firms = [
                {k: v for k, v in f.items() if k != "id"} for f in ks_firms_raw
            ]
            _save_ks_courts_cache(ks_firms, cache_path)
        except Exception as e:
            print(f"  [enhance] KS Courts scraper failed: {e}")
            return 0

    county_cities = {c.lower() for c in county_config["cities"]}
    enriched = 0
    added = 0

    for ks_firm in ks_firms:
        addr = ks_firm.get("address") or {}
        city = addr.get("city", "")
        if city.lower() not in county_cities:
            continue

        existing = _find_matching_firm(ks_firm["name"], city, firms)
        if existing:
            if not existing.get("phone") and ks_firm.get("phone"):
                existing["phone"] = ks_firm["phone"]
            e_addr = existing.get("address") or {}
            if not e_addr.get("street") and addr.get("street"):
                e_addr["street"] = addr["street"]
            if not e_addr.get("zip") and addr.get("zip"):
                e_addr["zip"] = addr["zip"]
            if ks_firm.get("attorneys"):
                existing.setdefault("attorneys", []).extend(ks_firm["attorneys"])
            _add_source(existing, "ks_courts")
            enriched += 1
        else:
            if not ks_firm.get("phone"):
                continue
            if not _looks_like_legal_entity(
                ks_firm["name"], [], ["ks_courts"],
            ):
                continue
            firms.append({
                "id": str(uuid.uuid4()),
                "name": ks_firm["name"],
                "practiceAreas": [],
                "summary": None,
                "website": None,
                "phone": ks_firm.get("phone"),
                "email": None,
                "address": addr,
                "coordinates": None,
                "sources": ["ks_courts"],
                "attorneys": ks_firm.get("attorneys", []),
                "google_business_profile": "",
            })
            added += 1

    print(f"  [enhance] KS Courts: enriched {enriched}, added {added} new firms (with phone)")
    return enriched + added


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
    to_scrape = [
        f for f in firms
        if f.get("website") and (not f.get("email") or not f.get("phone"))
    ]
    scraped = 0
    phones_found = 0
    emails_found = 0

    for firm in to_scrape:
        try:
            result = scrape_firm_website(
                firm["website"], firm["name"],
                (firm.get("address") or {}).get("city", "")
            )
        except Exception:
            continue

        if result.get("email") and not firm.get("email"):
            firm["email"] = result["email"]
            emails_found += 1
        if result.get("phone") and not firm.get("phone"):
            firm["phone"] = result["phone"]
            phones_found += 1
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

    print(f"  [enhance] Website scraping: {scraped} sites → {emails_found} emails, {phones_found} phones")
    return scraped


# ---------------------------------------------------------------------------
# Sub-step 7: Web search for missing websites
# ---------------------------------------------------------------------------

_DIRECTORY_DOMAINS = frozenset({
    "findlaw.com", "avvo.com", "justia.com", "lawyers.com", "martindale.com",
    "yelp.com", "yellowpages.com", "superlawyers.com", "nolo.com", "lawinfo.com",
    "hg.org", "lawyer.com", "bestlawyers.com", "usnews.com", "thumbtack.com",
    "facebook.com", "linkedin.com", "twitter.com", "instagram.com", "bbb.org",
    "google.com", "bing.com", "manta.com", "ksbar.org", "kscourts.org",
    "superpages.com", "whitepages.com", "duckduckgo.com", "wikipedia.org",
    "youtube.com", "trellis.law", "myftpupload.com", "wixsite.com",
    "squarespace.com", "weebly.com", "wordpress.com", "godaddy.com",
    "chamberofcommerce.com", "birdeye.com", "attorneyslisted.com",
    "lawyerdb.org", "showmelocal.com", "mapquest.com", "hub.biz",
    "local.yahoo.com", "citysearch.com", "lawyerlegion.com",
    "attorneyhelp.org", "attorneypages.com", "topattorney.com",
    "repsight.com", "trustanalytica.org", "locaterecords.com",
})

_SEARCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def _ddg_search(query: str, delay: float = 2.5) -> list[str]:
    from urllib.parse import quote_plus, unquote
    url = f"https://lite.duckduckgo.com/lite/?q={quote_plus(query)}"
    try:
        r = requests.get(url, timeout=15, headers=_SEARCH_HEADERS)
        time.sleep(delay)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "lxml")
    except Exception:
        return []

    results = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "uddg=" in href:
            m = re.search(r"uddg=([^&]+)", href)
            if m:
                actual = unquote(m.group(1))
                if actual.startswith("http"):
                    results.append(actual)
        elif href.startswith("http") and "duckduckgo.com" not in href:
            results.append(href)

    return results[:15]


def _is_directory_domain(url: str) -> bool:
    from urllib.parse import urlparse
    try:
        domain = urlparse(url).netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]
        for d in _DIRECTORY_DOMAINS:
            if domain == d or domain.endswith("." + d):
                return True
    except Exception:
        return True
    return False


def _pick_best_url(urls: list[str], firm_name: str) -> str | None:
    norm = normalize_firm_name(firm_name).lower()
    words = [w for w in norm.split() if len(w) > 2]
    from urllib.parse import urlparse

    best, best_score = None, -1
    for url in urls:
        if _is_directory_domain(url):
            continue
        try:
            domain = urlparse(url).netloc.lower()
            if domain.startswith("www."):
                domain = domain[4:]
        except Exception:
            continue

        score = 0
        base = domain.split(".")[0]
        for w in words:
            if w in base:
                score += 3
        if any(domain.endswith(t) for t in (".law", ".legal", ".attorney")):
            score += 2
        elif domain.endswith(".com"):
            score += 1

        if score > best_score:
            best, best_score = url, score
    return best


def _validate_search_url(url: str) -> str | None:
    try:
        r = requests.head(
            url, timeout=6, headers=_SEARCH_HEADERS, allow_redirects=True,
        )
        if r.status_code < 400:
            final = r.url
            if not _is_directory_domain(final):
                return final
    except Exception:
        pass
    return None


def _enrich_websites_via_search(firms: list) -> int:
    to_search = [
        f for f in firms
        if not f.get("website") and _LEGAL_RE.search(f.get("name", "").lower())
    ]
    if not to_search:
        return 0

    print(f"  [enhance] Web search: {len(to_search)} firms without websites to search")
    found = 0

    for i, firm in enumerate(to_search):
        city = (firm.get("address") or {}).get("city", "")
        query = f'{firm["name"]} {city} KS attorney'

        urls = _ddg_search(query, delay=2.5)
        if not urls:
            continue

        best = _pick_best_url(urls, firm["name"])
        if not best:
            continue

        validated = _validate_search_url(best)
        if validated:
            firm["website"] = validated
            _add_source(firm, "web_search")
            found += 1

        if (i + 1) % 25 == 0:
            print(f"  [enhance] Web search progress: {i + 1}/{len(to_search)}, found {found}")

    print(f"  [enhance] Web search: found {found} websites")
    return found


# ---------------------------------------------------------------------------
# Sub-step 8: Consolidate person-named entries into parent firms
# ---------------------------------------------------------------------------

_FIRM_TOKENS = (
    " llc", " llp", " l.l.p", " l.l.c", " pa", " p.a.",
    " law", " firm", " office", " associates", " group",
    " & ", " chartered", " attorneys", " partners", " pllc",
    " lc", " l.c.",
)


def _is_person_like(name: str) -> bool:
    if not name:
        return False
    lower = name.lower()
    if any(t in lower for t in _FIRM_TOKENS):
        return False
    if any(c.isdigit() for c in name):
        return False
    words = name.replace(",", " ").split()
    if len(words) < 2 or len(words) > 4:
        return False
    cap_words = sum(1 for w in words if w and w[0].isupper())
    return cap_words == len(words)


def _norm_phone(phone) -> str:
    if not phone:
        return ""
    return re.sub(r"\D", "", str(phone))


def _norm_street(street) -> str:
    if not street:
        return ""
    s = street.lower().strip()
    s = re.sub(r"\b(street|st)\b\.?", "st", s)
    s = re.sub(r"\b(avenue|ave)\b\.?", "ave", s)
    s = re.sub(r"\b(drive|dr)\b\.?", "dr", s)
    s = re.sub(r"\b(road|rd)\b\.?", "rd", s)
    s = re.sub(r"\b(boulevard|blvd)\b\.?", "blvd", s)
    s = re.sub(r"\b(suite|ste)\b\.?\s*\d+", "", s)
    s = re.sub(r"#\s*\d+", "", s)
    return re.sub(r"\s+", " ", s).strip()


def _consolidate_persons(firms: list) -> list:
    from collections import defaultdict

    firms_real = []
    persons = []
    for f in firms:
        (persons if _is_person_like(f.get("name", "")) else firms_real).append(f)

    if not persons:
        return firms

    by_phone = defaultdict(list)
    by_addr = defaultdict(list)
    for f in firms_real:
        p = _norm_phone(f.get("phone"))
        if p and len(p) >= 10:
            by_phone[p[-10:]].append(f)
        addr = f.get("address") or {}
        street = _norm_street(addr.get("street"))
        city = (addr.get("city") or "").lower()
        if street and city:
            by_addr[(street, city)].append(f)

    merged = 0
    dropped_no_contact = 0
    kept_solo = 0

    remaining = []
    for p in persons:
        if p.get("website"):
            kept_solo += 1
            remaining.append(p)
            continue

        phone = _norm_phone(p.get("phone"))
        addr = p.get("address") or {}
        street = _norm_street(addr.get("street"))
        city = (addr.get("city") or "").lower()

        candidates = []
        if phone and len(phone) >= 10:
            candidates.extend(by_phone.get(phone[-10:], []))
        if street and city:
            candidates.extend(by_addr.get((street, city), []))

        seen_ids = set()
        uniq = []
        for c in candidates:
            cid = id(c)
            if cid not in seen_ids:
                seen_ids.add(cid)
                uniq.append(c)

        if not uniq:
            has_contact = p.get("phone") or p.get("email") or p.get("website")
            if has_contact:
                kept_solo += 1
                remaining.append(p)
            else:
                dropped_no_contact += 1
            continue

        if len(uniq) > 1:
            names = {c.get("name") for c in uniq}
            if len(names) > 1:
                has_contact = p.get("phone") or p.get("email") or p.get("website")
                if has_contact:
                    kept_solo += 1
                    remaining.append(p)
                else:
                    dropped_no_contact += 1
                continue

        target = uniq[0]
        attorneys = target.setdefault("attorneys", [])
        pname = p.get("name", "").strip()
        existing_names = {
            (a.get("name", "") if isinstance(a, dict) else a).lower()
            for a in attorneys
        }
        if pname and pname.lower() not in existing_names:
            attorneys.append(pname)
        if p.get("phone") and not target.get("phone"):
            target["phone"] = p["phone"]
        if p.get("email") and not target.get("email"):
            target["email"] = p["email"]
        merged += 1

    result = firms_real + remaining
    print(f"  [enhance] Consolidation: {merged} merged into firms, "
          f"{kept_solo} kept as solo, {dropped_no_contact} dropped (no contact)")
    return result


# ---------------------------------------------------------------------------
# Main enhancement coordinator
# ---------------------------------------------------------------------------

def enhance_firms(
    firms: list, county_config: dict,
    test_mode: bool = False, skip_ks_courts: bool = False,
) -> list:
    print(f"\n[enhance] Starting enhancement for {county_config['name']}...")
    print(f"[enhance] {len(firms)} firms to enhance\n")

    if not skip_ks_courts:
        _enrich_from_ks_courts(firms, county_config, test_mode=test_mode)

    if not test_mode:
        _enrich_martindale(firms, county_config)
        _enrich_justia(firms, county_config)
        _enrich_avvo(firms, county_config)
        _enrich_findlaw(firms, county_config)
        _scrape_websites(firms)
        _enrich_websites_via_search(firms)
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

    # Sanitize names and addresses
    sanitized = []
    sanitize_dropped = 0
    for firm in firms:
        cleaned = _sanitize_firm_name(firm["name"])
        if cleaned is None:
            sanitize_dropped += 1
            continue
        firm["name"] = cleaned
        firm["address"] = _sanitize_address(firm.setdefault("address", {}))
        sanitized.append(firm)
    if sanitize_dropped:
        print(f"  [enhance] Sanitization: dropped {sanitize_dropped} corrupted entries")
    firms = sanitized

    # Consolidate person-named entries into parent firms
    firms = _consolidate_persons(firms)

    # Legal entity filter: remove non-law businesses regardless of source
    before_legal = len(firms)
    legal_filtered = []
    for firm in firms:
        if _looks_like_legal_entity(
            firm["name"],
            firm.get("practiceAreas", []),
            firm.get("sources"),
        ):
            legal_filtered.append(firm)
    dropped_legal = before_legal - len(legal_filtered)
    if dropped_legal:
        print(f"  [enhance] Legal filter: dropped {dropped_legal} non-law businesses")
    firms = legal_filtered

    # Quality gate: drop entries with zero contact info
    before_gate = len(firms)
    verified_sources = {"google_places", "foursquare"}
    gated = []
    for firm in firms:
        has_contact = (
            firm.get("website") or firm.get("phone") or firm.get("email")
            or firm.get("google_business_profile")
            or firm.get("legal_directory_listing")
        )
        if has_contact:
            gated.append(firm)
            continue
        firm_sources = set(firm.get("sources") or [])
        if firm_sources & verified_sources:
            gated.append(firm)
            continue
    dropped_gate = before_gate - len(gated)
    if dropped_gate:
        print(f"  [enhance] Quality gate: dropped {dropped_gate} entries with zero contact info")
    firms = gated

    # Set county on firms whose city is in the county; filter out the rest
    county_name = county_config["name"].replace(" County", "")
    county_state = county_config["state"]
    county_cities_lower = {c.lower() for c in county_config["cities"]}
    county_zips = set(county_config.get("zip_codes", []))
    filtered = []
    for firm in firms:
        addr = firm.setdefault("address", {})
        firm_zip = addr.get("zip", "")
        firm_state = addr.get("state", "")
        firm_city = addr.get("city", "").lower()

        in_county_by_zip = county_zips and firm_zip in county_zips
        in_county_by_city = (
            firm_city
            and firm_city in county_cities_lower
            and (not firm_state or firm_state == county_state)
        )

        if not in_county_by_zip and not in_county_by_city:
            continue

        if in_county_by_zip and firm_state and firm_state != county_state:
            addr["state"] = county_state
        if in_county_by_zip and firm_city not in county_cities_lower:
            for c in county_config["cities"]:
                if c.lower() == "kansas city":
                    addr["city"] = c
                    break

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
