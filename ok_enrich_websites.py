#!/usr/bin/env python3
"""
Find websites for all Oklahoma law firms that don't have one.
Runs DDG search for every no-website firm across all 14 OK county CSVs.

Usage: python3 ok_enrich_websites.py [slug ...]
       python3 ok_enrich_websites.py          # all OK counties
"""
import csv
import re
import sys
import time
import warnings
from pathlib import Path
from urllib.parse import quote_plus, unquote, urlparse

import requests
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore", message="Unverified HTTPS request")

DATA_DIR = Path("app/county-data")

_SEARCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

_DIRECTORY_DOMAINS = frozenset({
    "findlaw.com", "avvo.com", "justia.com", "lawyers.com", "martindale.com",
    "yelp.com", "yellowpages.com", "superlawyers.com", "nolo.com", "lawinfo.com",
    "hg.org", "lawyer.com", "bestlawyers.com", "usnews.com", "thumbtack.com",
    "facebook.com", "linkedin.com", "twitter.com", "instagram.com", "bbb.org",
    "google.com", "bing.com", "manta.com", "okbar.org", "ams.okbar.org",
    "superpages.com", "whitepages.com", "duckduckgo.com", "wikipedia.org",
    "youtube.com", "trellis.law", "wixsite.com", "squarespace.com",
    "weebly.com", "wordpress.com", "godaddy.com", "chamberofcommerce.com",
    "birdeye.com", "attorneyslisted.com", "lawyerdb.org", "showmelocal.com",
    "mapquest.com", "hub.biz", "local.yahoo.com", "citysearch.com",
    "lawyerlegion.com", "attorneyhelp.org", "attorneypages.com",
    "topattorney.com", "repsight.com", "trustanalytica.org", "locaterecords.com",
    "mannfordmap.com", "rymaps.xyz", "rogerscountybar.org",
})

_BAD_URL_RE = re.compile(
    r'facebook\.com|instagram\.com|twitter\.com|linkedin\.com|yelp\.com|'
    r'yellowpages\.com|google\.com/maps|avvo\.com|justia\.com|martindale\.com|'
    r'findlaw\.com|lawyers\.com|okbar\.org|bbb\.org|superlawyers\.com|'
    r'ams\.okbar\.org|rogerscountybar\.org',
    re.IGNORECASE,
)

_UTM_RE = re.compile(r'[?&](utm_|gclid|fbclid|authuser|npcmp|ref=|_ga=)')


def _is_directory(url: str) -> bool:
    if not url:
        return True
    if _BAD_URL_RE.search(url):
        return True
    try:
        domain = urlparse(url).netloc.lower().lstrip("www.")
        for d in _DIRECTORY_DOMAINS:
            if domain == d or domain.endswith("." + d):
                return True
    except Exception:
        return True
    return False


def _ddg_search(query: str, delay: float = 2.5) -> list[str]:
    url = f"https://lite.duckduckgo.com/lite/?q={quote_plus(query)}"
    try:
        r = requests.get(url, timeout=15, headers=_SEARCH_HEADERS)
        time.sleep(delay)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "lxml")
    except Exception:
        time.sleep(delay)
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


def _norm_name(s: str) -> str:
    s = re.sub(r'\b(llc|pllc|llp|pc|p\.c\.|p\.a\.|pa|lc|ltd|inc|co\.?)\b', '', s.lower())
    return re.sub(r'[^a-z0-9 ]', '', s).strip()


def _pick_best_url(urls: list[str], firm_name: str) -> str | None:
    norm = _norm_name(firm_name)
    words = [w for w in norm.split() if len(w) > 2]

    best, best_score = None, -1
    for url in urls:
        if _is_directory(url):
            continue
        try:
            domain = urlparse(url).netloc.lower().lstrip("www.")
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


def _validate_url(url: str) -> str | None:
    try:
        r = requests.head(url, timeout=6, headers=_SEARCH_HEADERS, allow_redirects=True,
                          verify=False)
        if r.status_code < 400:
            final = r.url
            if not _is_directory(final) and not _UTM_RE.search(final):
                return final
            if not _is_directory(final):
                # Strip UTM params
                from urllib.parse import urlparse as _up, parse_qs, urlencode, urlunparse
                parts = _up(final)
                params = {k: v for k, v in parse_qs(parts.query).items()
                          if not k.lower().startswith(('utm_', 'gclid', 'fbclid', '_ga', 'authuser', 'npcmp'))}
                clean_q = urlencode(params, doseq=True)
                return urlunparse(parts._replace(query=clean_q))
    except Exception:
        pass
    return None


def enrich_county(slug: str) -> dict:
    p = DATA_DIR / f"{slug}.csv"
    if not p.exists():
        print(f"  SKIP: {p} not found")
        return {}
    rows = list(csv.DictReader(p.open()))
    if not rows:
        return {}
    fieldnames = list(rows[0].keys())

    no_web = [r for r in rows if not r.get("website", "").strip()]
    if not no_web:
        print(f"  {slug}: all firms have websites")
        return {"found": 0, "searched": 0}

    print(f"  {slug}: searching {len(no_web)} no-website firms...")
    found = 0

    for i, row in enumerate(no_web):
        name = row.get("law_firm_name", "").strip()
        city = row.get("city", "").strip()
        query = f'"{name}" {city} Oklahoma attorney law'

        urls = _ddg_search(query, delay=2.0)
        if not urls:
            continue

        best = _pick_best_url(urls, name)
        if not best:
            continue

        validated = _validate_url(best)
        if validated:
            row["website"] = validated
            found += 1

        if (i + 1) % 20 == 0:
            print(f"    progress: {i+1}/{len(no_web)}, found {found} websites")

    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"  {slug}: +{found} websites ({found}/{len(no_web)} found)")
    return {"found": found, "searched": len(no_web)}


if __name__ == "__main__":
    if sys.argv[1:]:
        slugs = sys.argv[1:]
    else:
        slugs = [p.stem for p in sorted(DATA_DIR.glob("*-ok.csv"))]

    print(f"Enriching websites for {len(slugs)} OK county CSV(s)...\n")
    total_found = total_searched = 0
    for slug in slugs:
        stats = enrich_county(slug)
        total_found += stats.get("found", 0)
        total_searched += stats.get("searched", 0)

    print(f"\nDone. Found {total_found} new websites out of {total_searched} firms searched.")
