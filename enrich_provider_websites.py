#!/usr/bin/env python3
"""
Three-pass website enrichment for provider CSVs (chiropractors & PTs):

Pass 1: DDG phone-number search — "(913) 555-1234 chiropractor" →
        finds the clinic's own site as first result (very targeted)
Pass 2: Domain guessing — normalize org name, try .com variants, verify via HTTP HEAD
Pass 3: DDG name+city search — standard query with 5s+ delays for remainder

Usage: python3 enrich_provider_websites.py
"""
import csv
import re
import socket
import time
import warnings
from pathlib import Path
from urllib.parse import quote_plus, urlparse

import requests
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore")

DATA_DIR = Path("app/county-data")

FIELDNAMES = [
    "provider_name", "website", "phone_number", "provider_type",
    "city", "state", "county", "street_address", "zip_code",
    "email", "npi_number",
]

DDG_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
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
    "psychology-today.com", "psychologytoday.com", "therapyfinder.com",
    "therapist.com", "goodtherapy.org", "indeed.com", "glassdoor.com",
    "chamberofcommerce.com", "birdeye.com", "trustpilot.com", "merchantcircle.com",
    "ezlocal.com", "showmelocal.com", "citysearch.com", "insiderpages.com",
    "doctor.com", "wellness.com", "healthline.com", "everydayhealth.com",
})


def _norm_domain(url: str) -> str:
    if not url:
        return ""
    try:
        return re.sub(r"^www\.", "", urlparse(url.strip()).netloc.lower())
    except Exception:
        return ""


def _is_bad(url: str) -> bool:
    if not url or not url.startswith("http"):
        return True
    d = _norm_domain(url)
    return any(d == b or d.endswith("." + b) for b in BAD_DOMAINS)


def _ddg_first_good(query: str) -> str:
    """Return first non-directory URL from DDG lite, or ''."""
    url = f"https://lite.duckduckgo.com/lite/?q={quote_plus(query)}"
    try:
        r = requests.get(url, headers=DDG_HEADERS, timeout=12)
        if r.status_code != 200:
            return ""
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("http") and not _is_bad(href):
                return href
        return ""
    except Exception:
        return ""


def _head_resolves(url: str, timeout: int = 6) -> bool:
    """True if an HTTP HEAD request gets a 2xx or 3xx response."""
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True,
                          verify=False,
                          headers={"User-Agent": DDG_HEADERS["User-Agent"]})
        return r.status_code < 400
    except Exception:
        return False


def _name_to_domain_candidates(name: str, city: str) -> list[str]:
    """
    Generate plausible domain names from a clinic name.
    E.g. "Mission Chiropractic LLC" → missionchiropractic.com etc.
    """
    # Strip legal suffixes
    cleaned = re.sub(
        r'\b(llc|pllc|inc|pa|ltd|corp|dba|d\.b\.a\.)\b',
        '', name, flags=re.I
    ).strip()
    # Remove credentials
    cleaned = re.sub(r',\s*(D\.?C\.?|DPT|PT|LPT|MSPT|MPT|DC)\s*$', '', cleaned, flags=re.I).strip()
    # Remove punctuation, collapse spaces
    base = re.sub(r"[^a-z0-9\s]", "", cleaned.lower())
    base = re.sub(r"\s+", "", base).strip()

    if not base or len(base) < 4:
        return []

    # City slug for local variants
    city_slug = re.sub(r"[^a-z0-9]", "", city.lower())

    candidates = []
    for ext in (".com", ".net", ".org"):
        candidates.append(f"https://{base}{ext}")
        candidates.append(f"https://www.{base}{ext}")

    # City-suffixed variants (for common names like "Family Chiropractic")
    if len(base) < 20:  # only for shorter names where city helps disambiguate
        for ext in (".com",):
            candidates.append(f"https://{base}{city_slug}{ext}")
            candidates.append(f"https://{city_slug}{base}{ext}")

    return candidates


def is_org_name(name: str) -> bool:
    """True if the name looks like a clinic/practice rather than a solo individual."""
    if re.search(r'\b(llc|pllc|inc|pa|ltd|corp)\b', name, re.I):
        return True
    org_kw = r'\b(chiropractic|physical\s+therapy|therapy|center|clinic|group|associates|health|wellness|rehab|sports|spine|family|care|institute|services|practice|network)\b'
    return len(re.findall(org_kw, name, re.I)) >= 1


# ---------------------------------------------------------------------------
# Pass 1: DDG phone search
# ---------------------------------------------------------------------------

def pass1_phone_ddg(records: list[dict]) -> int:
    targets = [r for r in records if not r["website"] and r["phone_number"]]
    print(f"\n  Pass 1 (DDG phone search): {len(targets)} records")
    found = 0
    for i, rec in enumerate(targets):
        phone = rec["phone_number"]
        ptype = rec["provider_type"].lower()
        query = f'{phone} {ptype}'
        site = _ddg_first_good(query)
        if site:
            rec["website"] = site
            found += 1
        time.sleep(4.0)  # conservative delay
        if (i + 1) % 25 == 0:
            print(f"    {i+1}/{len(targets)} — {found} found")
    print(f"    Done: {found} websites via phone search")
    return found


# ---------------------------------------------------------------------------
# Pass 2: Domain guessing + HTTP verification
# ---------------------------------------------------------------------------

def pass2_domain_guess(records: list[dict]) -> int:
    targets = [r for r in records if not r["website"] and is_org_name(r["provider_name"])]
    print(f"\n  Pass 2 (domain guessing): {len(targets)} org-name records")
    found = 0
    for rec in targets:
        candidates = _name_to_domain_candidates(rec["provider_name"], rec["city"])
        for url in candidates:
            if _head_resolves(url):
                rec["website"] = url
                found += 1
                break
        time.sleep(0.3)  # HEAD requests are cheap
    print(f"    Done: {found} websites via domain guessing")
    return found


# ---------------------------------------------------------------------------
# Pass 3: DDG name + city search (org names only, longer delay)
# ---------------------------------------------------------------------------

def pass3_name_ddg(records: list[dict]) -> int:
    targets = [r for r in records if not r["website"] and is_org_name(r["provider_name"])]
    print(f"\n  Pass 3 (DDG name+city): {len(targets)} records")
    found = 0
    for i, rec in enumerate(targets):
        name = rec["provider_name"]
        city = rec["city"]
        state = rec["state"]
        ptype = rec["provider_type"].lower()
        query = f'"{name}" {ptype} {city} {state}'
        site = _ddg_first_good(query)
        if site:
            rec["website"] = site
            found += 1
        time.sleep(5.0)
        if (i + 1) % 20 == 0:
            print(f"    {i+1}/{len(targets)} — {found} found")
    print(f"    Done: {found} websites via name+city search")
    return found


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    for p in sorted(DATA_DIR.glob("providers-*.csv")):
        print(f"\n{'='*55}")
        print(f"{p.stem}")
        print(f"{'='*55}")

        rows = list(csv.DictReader(p.open()))
        before = sum(1 for r in rows if r.get("website", "").strip())
        print(f"  Starting: {len(rows)} records, {before} with website")

        # Run all three passes
        found1 = pass1_phone_ddg(rows)
        # Save after each pass in case of interruption
        _write(p, rows)

        found2 = pass2_domain_guess(rows)
        _write(p, rows)

        found3 = pass3_name_ddg(rows)
        _write(p, rows)

        after = sum(1 for r in rows if r.get("website", "").strip())
        print(f"\n  Final: {after} websites (+{after - before} from {before})")
        print(f"  Breakdown: phone={found1}, domain={found2}, name={found3}")


def _write(path: Path, rows: list[dict]):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(rows)


if __name__ == "__main__":
    main()
