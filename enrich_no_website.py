#!/usr/bin/env python3
"""
Enrich entries that have no website by:
1. Searching DuckDuckGo for their website
2. For entries with GBP, fetching the Google Maps page via Playwright
   to extract business category / description for practice area hints.

Usage: python3 enrich_no_website.py
"""

import csv
import json
import re
import sys
import time
import warnings
from pathlib import Path
from urllib.parse import quote_plus, urljoin, urlparse

import requests

from law_domain_guess import find_website_by_guessing
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore", message="Unverified HTTPS request")

DATA_DIR = Path("app/county-data")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

PRACTICE_KEYWORDS = {
    "Personal Injury": ["personal injury", "car accident", "auto accident", "wrongful death", "accident attorney"],
    "Family Law": ["family law", "divorce attorney", "child custody", "family attorney"],
    "Criminal Defense": ["criminal defense", "criminal attorney", "dui attorney", "dwi", "criminal law"],
    "DUI": ["dui", "dwi", "drunk driving", "driving while intoxicated"],
    "Estate Planning": ["estate planning", "wills and trusts", "probate attorney", "elder law"],
    "Workers' Compensation": ["workers comp", "workers' compensation", "work injury"],
    "Bankruptcy": ["bankruptcy attorney", "chapter 7", "chapter 13", "debt relief attorney"],
    "Business Law": ["business attorney", "corporate attorney", "business law"],
    "Real Estate": ["real estate attorney", "real estate law"],
    "Immigration": ["immigration attorney", "immigration lawyer", "visa attorney"],
    "Employment Law": ["employment attorney", "wrongful termination", "discrimination attorney"],
    "Medical Malpractice": ["medical malpractice", "medical negligence"],
    "Social Security Disability": ["social security disability", "ssdi attorney"],
    "Tax Law": ["tax attorney", "irs attorney", "tax law"],
    "Intellectual Property": ["trademark attorney", "patent attorney", "intellectual property"],
}

PRIORITY_SCORES = {
    "Criminal Defense": 5, "DUI": 5, "Personal Injury": 5,
    "Medical Malpractice": 5, "Workers' Compensation": 5,
    "Sexual Assault": 4, "Family Law": 4, "General Practice": 4,
    "Employment Law": 3, "Civil Litigation": 3,
    "Estate Planning": 2, "Bankruptcy": 2,
    "Real Estate": 2, "Business Law": 2,
    "Immigration": 2, "Tax Law": 2, "Social Security Disability": 2,
    "Intellectual Property": 1,
}

_SKIP_DOMAINS = {
    "google.com", "yelp.com", "avvo.com", "martindale.com", "findlaw.com",
    "justia.com", "lawyers.com", "superlawyers.com", "bing.com",
    "facebook.com", "linkedin.com", "instagram.com", "twitter.com",
    "yellowpages.com", "whitepages.com", "bbb.org", "angieslist.com",
    "foursquare.com", "mapquest.com", "nextdoor.com",
}


def guess_practice(text: str) -> str:
    lower = text.lower()
    for area, kws in PRACTICE_KEYWORDS.items():
        if any(kw in lower for kw in kws):
            return area
    return "General"


def _fetch(url: str, timeout: int = 8) -> requests.Response | None:
    for verify in (True, False):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout,
                             verify=verify, allow_redirects=True)
            if r.status_code == 200:
                return r
        except Exception:
            if verify:
                continue
    return None


def _is_law_website(url: str) -> bool:
    """Quick check that a URL looks like a law firm site, not a directory."""
    domain = urlparse(url).netloc.lower().lstrip("www.")
    return not any(skip in domain for skip in _SKIP_DOMAINS)


# ---------------------------------------------------------------------------
# DuckDuckGo search for websites
# ---------------------------------------------------------------------------

def search_ddg(query: str) -> list[str]:
    """Return up to 3 non-directory URLs from a DuckDuckGo search."""
    encoded = quote_plus(query)
    url = f"https://html.duckduckgo.com/html/?q={encoded}"
    resp = _fetch(url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    urls = []
    for a in soup.find_all("a", href=True, class_="result__url"):
        href = a["href"]
        if not href.startswith("http"):
            href = "https://" + href.lstrip("/")
        if _is_law_website(href):
            urls.append(href)
        if len(urls) >= 3:
            break

    # Also check result__a links
    if not urls:
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if "uddg=" in href:
                # DDG redirect URL
                m = re.search(r'uddg=([^&]+)', href)
                if m:
                    from urllib.parse import unquote
                    real_url = unquote(m.group(1))
                    if _is_law_website(real_url) and real_url not in urls:
                        urls.append(real_url)
            if len(urls) >= 3:
                break

    return urls


def find_website_via_search(name: str, city: str, state: str) -> str | None:
    """Try to find a law firm's website via web search."""
    query = f'"{name}" {city} {state} attorney lawyer site'
    results = search_ddg(query)
    if not results:
        # Try simpler query
        query2 = f'{name} {city} {state} law'
        results = search_ddg(query2)

    for url in results:
        # Quick validation: fetch and check for law content
        resp = _fetch(url, timeout=5)
        if resp:
            text = resp.text.lower()
            if any(kw in text for kw in ["attorney", "lawyer", "law firm", "legal services"]):
                return url
    return None


# ---------------------------------------------------------------------------
# GBP practice area via Playwright
# ---------------------------------------------------------------------------

def extract_practice_from_gbp(place_id_url: str) -> str:
    """Use Playwright to fetch a Google Maps page and extract practice hints."""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.set_extra_http_headers(HEADERS)
            page.goto(place_id_url, timeout=15000, wait_until="domcontentloaded")
            page.wait_for_timeout(2000)
            content = page.content()
            browser.close()
        practice = guess_practice(content)
        return practice if practice != "General" else ""
    except Exception as e:
        return ""


# ---------------------------------------------------------------------------
# Main enrichment
# ---------------------------------------------------------------------------

def enrich_no_website(slug: str) -> dict:
    path = DATA_DIR / f"{slug}.csv"
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    fieldnames = list(rows[0].keys())

    # Targets: no website
    no_web = [r for r in rows if not r["website"].strip()]
    print(f"\n[{slug}] {len(no_web)} entries without websites")

    has_gbp = [r for r in no_web if r["google_business_profile"].strip()]
    no_gbp_either = [r for r in no_web if not r["google_business_profile"].strip()]

    print(f"  Has GBP (try Playwright): {len(has_gbp)}")
    print(f"  No GBP (try search): {len(no_gbp_either)}")

    stats = {"website_found": 0, "practice_updated": 0}

    # 1. GBP → Playwright practice area
    for i, row in enumerate(has_gbp):
        if row["practice_area"] != "General":
            continue
        gbp_url = row["google_business_profile"]
        practice = extract_practice_from_gbp(gbp_url)
        if practice:
            row["practice_area"] = practice
            row["priority"] = str(PRIORITY_SCORES.get(practice, 1))
            stats["practice_updated"] += 1
        if i % 10 == 9:
            print(f"  GBP progress: {i+1}/{len(has_gbp)}, updated {stats['practice_updated']}")
        time.sleep(0.5)

    print(f"  GBP done: {stats['practice_updated']} practice areas from GBP")

    # 2. High-precision domain guessing for firms with no website and no GBP.
    # NOTE: DuckDuckGo (the old find_website_via_search backend) is unreachable
    # from this environment (confirmed connection timeout at the network level,
    # not "no results") — every prior run silently found nothing here. Domain
    # guessing doesn't depend on any search engine and validates against the
    # firm's actual city/zip/phone before accepting a match, so it can't
    # false-positive on a same-named firm in a different state.
    search_targets = no_gbp_either
    print(f"  Guessing websites for {len(search_targets)} firms...")
    search_updated = 0
    for i, row in enumerate(search_targets):
        name = row["law_firm_name"]
        city = row["city"]
        state = row["state"]
        website = find_website_by_guessing(name, city, state, row.get("zip_code", ""), row.get("phone_number", ""))
        if website:
            row["website"] = website
            stats["website_found"] += 1
            search_updated += 1
            # Also try practice area from newly found site
            resp = _fetch(website, timeout=5)
            if resp:
                practice = guess_practice(resp.text)
                if practice != "General":
                    row["practice_area"] = practice
                    row["priority"] = str(PRIORITY_SCORES.get(practice, 1))
                    stats["practice_updated"] += 1
        if i % 10 == 9:
            print(f"  Search progress: {i+1}/{len(search_targets)}, websites found: {stats['website_found']}")

    # Save
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    mpath = DATA_DIR / "manifest.json"
    manifest = json.loads(mpath.read_text())
    from datetime import date
    today = date.today().isoformat()
    for c in manifest["counties"]:
        if c["slug"] == slug:
            c["last_updated"] = today
            break
    mpath.write_text(json.dumps(manifest, indent=2) + "\n")

    return stats


if __name__ == "__main__":
    slugs = ["jackson-county-mo", "greene-county-mo", "st-charles-county-mo"]
    if "--county" in sys.argv:
        idx = sys.argv.index("--county")
        slugs = [sys.argv[idx + 1]]

    for slug in slugs:
        print(f"\n{'='*50}")
        print(f"  {slug}")
        print(f"{'='*50}")
        s = enrich_no_website(slug)
        print(f"\n  Summary: websites_found={s['website_found']}, practice_updated={s['practice_updated']}")
