#!/usr/bin/env python3
"""
Provider website enrichment - Pass 2:
- Org-name providers: Startpage search for real clinic website
- Individual-name providers: Google Maps (GBP) link as fallback
- Any org that Startpage misses: GBP fallback too
"""
import csv
import re
import time
from pathlib import Path
from urllib.parse import quote_plus, urlparse, quote

import requests
from bs4 import BeautifulSoup

DATA_DIR = Path("app/county-data")

FIELDNAMES = [
    "provider_name", "website", "phone_number", "provider_type",
    "city", "state", "county", "street_address", "zip_code",
    "email", "npi_number",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
}

BAD_DOMAINS = frozenset({
    "healthgrades.com", "zocdoc.com", "vitals.com", "ratemds.com", "webmd.com",
    "doximity.com", "yelp.com", "yellowpages.com", "superpages.com", "whitepages.com",
    "facebook.com", "linkedin.com", "twitter.com", "instagram.com", "google.com",
    "bing.com", "wikipedia.org", "youtube.com", "bbb.org", "manta.com", "mapquest.com",
    "npiprofile.com", "npino.com", "npinumber.org", "npidb.org", "medicare.gov",
    "cms.gov", "npiregistry.cms.hhs.gov", "usnews.com", "castleconnolly.com",
    "sharecare.com", "duckduckgo.com", "angieslist.com", "homeadvisor.com",
    "thumbtack.com", "care.com", "practicefusion.com", "chirodirectory.com",
    "apta.org", "acatoday.org", "americanchiropractors.org", "chiroeco.com",
    "findachiropractor.com", "chiromatrix.com", "chiropractic.org",
    "psychologytoday.com", "therapyfinder.com", "therapist.com", "goodtherapy.org",
    "indeed.com", "glassdoor.com", "chamberofcommerce.com", "birdeye.com",
    "trustpilot.com", "merchantcircle.com", "ezlocal.com", "showmelocal.com",
    "citysearch.com", "insiderpages.com", "doctor.com", "wellness.com",
    "healthline.com", "everydayhealth.com", "startpage.com", "classpass.com",
    "mindbodyonline.com", "vagaro.com", "reddit.com", "tiktok.com", "pinterest.com",
    "hg.org", "avvo.com", "justia.com", "findlaw.com", "lawyerlegion.com",
    "npinumber.org", "npidb.org", "chiromatrix.com", "wellnessliving.com",
    "nextdoor.com", "patch.com", "ksbn.net", "ksbha.org",
})

ORG_KEYWORDS = re.compile(
    r'\b(chiropractic|physical\s+therapy|therapy|therapist|center|clinic|group|associates|'
    r'health|wellness|rehab|rehabilitation|sports|spine|spinal|family|care|institute|'
    r'services|practice|network|acupuncture|massage|fitness|orthopedic|medical|injury|'
    r'back|pain|movement|motion|performance|physical|manual|balance|core|active|'
    r'integrated|advanced|premier|elite|professional|optimal|comprehensive)\b',
    re.I,
)

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def is_org(name: str) -> bool:
    return bool(ORG_KEYWORDS.search(name))


def clean_name_for_search(name: str) -> str:
    """Strip legal suffixes and credentials from a name for search."""
    out = re.sub(r',?\s*\b(llc|pllc|inc|pa|ltd|corp|dba|d\.b\.a\.)\b', '', name, flags=re.I)
    out = re.sub(r',?\s*\b(d\.?c\.?|dpt|pt|lpt|mspt|mpt|dc|md|do|np|ot)\b', '', out, flags=re.I)
    out = re.sub(r'\s+', ' ', out).strip().strip(',').strip()
    return out


def _norm_domain(url: str) -> str:
    try:
        return re.sub(r"^www\.", "", urlparse(url.strip()).netloc.lower())
    except Exception:
        return ""


def _is_bad(url: str) -> bool:
    if not url or not url.startswith("http"):
        return True
    d = _norm_domain(url)
    return any(d == b or d.endswith("." + b) for b in BAD_DOMAINS)


_GENERIC_DOMAIN_PREFIX = re.compile(
    r'^(chiropractor|physicaltherapist|physicaltherapy|physio|backpain|spinecare|spinedoctor)',
    re.I,
)

# Generic words that appear in many clinic names — not distinctive enough to identify a specific clinic
_SKIP_WORDS = frozenset({
    "chiropractic", "chiropractor", "therapy", "therapist", "therapies",
    "physical", "center", "clinic", "group", "health", "wellness", "rehab",
    "rehabilitation", "sports", "spine", "spinal", "family", "care", "institute",
    "services", "practice", "network", "acupuncture", "massage", "fitness",
    "orthopedic", "medical", "injury", "back", "pain", "movement", "motion",
    "performance", "manual", "balance", "core", "active", "integrated", "advanced",
    "premier", "elite", "professional", "optimal", "comprehensive", "county",
    "associates", "johnson", "overland", "prairie", "village", "mission",
})


def _domain_relevant(url: str, name: str, city: str) -> bool:
    """Check if the URL domain plausibly belongs to this specific provider."""
    # Reject non-web resources
    if re.search(r'\.(pdf|zip|doc|docx|xls|ppt|jpg|png)$', url, re.I):
        return False
    domain_raw = _norm_domain(url)
    domain = domain_raw.replace("-", "").replace(".", "")

    # Distinctive words from the cleaned name (5+ chars, not generic)
    cleaned = clean_name_for_search(name).lower()
    name_words = [
        w for w in re.split(r'\s+', cleaned)
        if len(w) >= 5 and w not in _SKIP_WORDS
    ]
    if any(w in domain for w in name_words):
        return True

    # City slug check — only accept if domain doesn't look like a generic directory
    city_slug = re.sub(r"[^a-z0-9]", "", city.lower())
    if len(city_slug) >= 4 and city_slug in domain:
        if _GENERIC_DOMAIN_PREFIX.match(domain_raw):
            return False
        # Also reject "{city}{generic_keyword}" patterns like "overlandparkchiropractic.com"
        _GEN_SUFFIX = re.compile(
            r'^' + re.escape(city_slug) + r'(chiropractic|physicaltherapy|physicaltherapist|chiropractor|therapy)',
            re.I,
        )
        if _GEN_SUFFIX.match(domain):
            return False
        return True

    return False


def startpage_search(name: str, city: str, state: str, ptype: str) -> str:
    """Search Startpage, return first real clinic website that relates to the provider."""
    cleaned = clean_name_for_search(name)
    if not cleaned:
        return ""
    query = f"{cleaned} {ptype} {city} {state}"
    url = f"https://www.startpage.com/sp/search?q={quote_plus(query)}&language=english"
    try:
        r = SESSION.get(url, timeout=15)
        if r.status_code != 200:
            return ""
        soup = BeautifulSoup(r.text, "html.parser")
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("http") and not _is_bad(href) and href not in seen:
                seen.add(href)
                if _domain_relevant(href, name, city):
                    return href
        return ""
    except Exception:
        return ""


def google_maps_link(name: str, city: str, state: str) -> str:
    """Google Maps search URL — reliable GBP fallback."""
    cleaned = clean_name_for_search(name)
    query = f"{cleaned or name} {city} {state}"
    return f"https://www.google.com/maps/search/?api=1&query={quote(query)}"


def _write(path: Path, rows: list[dict]):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(rows)


def enrich_file(path: Path):
    rows = list(csv.DictReader(path.open()))
    targets = [r for r in rows if not r.get("website", "").strip()]

    org_targets = [r for r in targets if is_org(r["provider_name"])]
    indiv_targets = [r for r in targets if not is_org(r["provider_name"])]

    print(f"\n{'='*55}")
    print(f"{path.stem}")
    print(f"  {len(rows)} total | {len(rows)-len(targets)} have website | {len(targets)} need enrichment")
    print(f"  Startpage searches: {len(org_targets)} org names")
    print(f"  GBP direct: {len(indiv_targets)} individual names")
    print(f"{'='*55}")

    startpage_found = 0
    gbp_added = 0

    # Org names: try Startpage, fall back to GBP
    for i, rec in enumerate(org_targets):
        name = rec["provider_name"]
        city = rec["city"]
        state = rec["state"]
        ptype = rec["provider_type"]

        site = startpage_search(name, city, state, ptype)
        if site:
            rec["website"] = site
            startpage_found += 1
        else:
            rec["website"] = google_maps_link(name, city, state)
            gbp_added += 1

        if (i + 1) % 25 == 0 or i + 1 == len(org_targets):
            print(f"  Startpage {i+1}/{len(org_targets)} — real: {startpage_found}, gbp so far: {gbp_added}", flush=True)
            _write(path, rows)

        time.sleep(3.5)

    # Individual names: GBP directly
    for rec in indiv_targets:
        rec["website"] = google_maps_link(
            rec["provider_name"], rec["city"], rec["state"]
        )
        gbp_added += 1

    _write(path, rows)

    after = sum(1 for r in rows if r.get("website", "").strip())
    print(f"\n  Completed: {after}/{len(rows)} have website/link")
    print(f"  Real websites found: {startpage_found}")
    print(f"  GBP fallbacks added: {gbp_added}")


def main():
    for p in sorted(DATA_DIR.glob("providers-*.csv")):
        enrich_file(p)
    print("\nAll done.")


if __name__ == "__main__":
    main()
