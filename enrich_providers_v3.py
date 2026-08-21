#!/usr/bin/env python3
"""
Pass 2 website enrichment for all 92 new KS county provider CSVs.

Strategy:
  1. Startpage search at 15s delay (confirmed working) for each org without website
  2. Global address/phone sharing pass after all counties
  3. Save progress after each county

Key fix vs. providers_ks_all.py: drop _domain_relevant() filter — it was
rejecting correct results (e.g. rappwellness.com for "Chiropractic Wellness
Center PA"). Startpage query (name + type + city + state) is specific enough
that first non-directory result is almost always the correct site.

Usage:
  python3 enrich_providers_v3.py              # all counties
  python3 enrich_providers_v3.py --resume     # skip counties already enriched
"""
import argparse
import csv
import re
import sys
import time
from pathlib import Path
from urllib.parse import quote_plus, urlparse

import requests

DATA_DIR = Path("app/county-data")

FIELDNAMES = [
    "provider_name", "website", "phone_number", "provider_type",
    "city", "state", "county", "street_address", "zip_code",
    "email", "npi_number",
]

_BAD_DOMAINS = frozenset({
    "healthgrades.com", "zocdoc.com", "vitals.com", "ratemds.com", "webmd.com",
    "doximity.com", "yelp.com", "yellowpages.com", "superpages.com", "whitepages.com",
    "facebook.com", "linkedin.com", "twitter.com", "instagram.com", "google.com",
    "bing.com", "wikipedia.org", "youtube.com", "bbb.org", "manta.com", "mapquest.com",
    "npiprofile.com", "npino.com", "npinumber.org", "npidb.org", "medicare.gov",
    "cms.gov", "npiregistry.cms.hhs.gov", "usnews.com", "castleconnolly.com",
    "sharecare.com", "duckduckgo.com", "startpage.com", "chirodirectory.com",
    "apta.org", "acatoday.org", "findachiropractor.com", "chiromatrix.com",
    "chiropractic.org", "doctor.com", "wellness.com", "healthline.com",
    "psychologytoday.com", "psychology-today.com", "birdeye.com",
    "practicefusion.com", "merchantcircle.com", "ezlocal.com", "showmelocal.com",
    "citysearch.com", "insiderpages.com", "chamberofcommerce.com", "angieslist.com",
    "homeadvisor.com", "thumbtack.com", "care.com", "therapyfinder.com",
    "therapist.com", "goodtherapy.org", "indeed.com", "glassdoor.com",
    "trustpilot.com", "foursquare.com", "maps.google.com", "wikidata.org",
    "dbpedia.org", "encyclopediaofkansas.org", "kansastravel.org",
    "npi.io", "nppes.com", "npidata.cms.hhs.gov",
    "chiroeco.com", "chirotouch.com", "thechiropractors.com",
    "pt-helper.com", "pthelper.com",
})

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

_SESSION = requests.Session()
_SESSION.headers.update(_HEADERS)

_ORG_RE = re.compile(
    r'\b(chiropractic|chiropractor|physical\s+therapy|therapy|therapist|center|clinic|'
    r'group|associates|health|wellness|rehab|sports|spine|family|care|institute|'
    r'services|practice|network|back|joint|llc|pllc|inc|pa|corp|acupuncture|'
    r'massage|orthopedic|medical|rehabilitation|movement|performance|solutions|'
    r'integrated|advanced|premier|optimal)\b', re.I
)


def _is_org(name: str) -> bool:
    return bool(_ORG_RE.search(name))


def _norm_domain(url: str) -> str:
    try:
        return re.sub(r"^www\.", "", urlparse(url.strip()).netloc.lower())
    except Exception:
        return ""


def _is_bad(url: str) -> bool:
    if not url or not url.startswith("http"):
        return True
    # Filter file attachments
    if re.search(r"\.(pdf|zip|doc|docx|xls|xlsx|csv|jpg|png|gif|mp4|mp3|ppt|pptx)(\?|$)", url, re.I):
        return True
    # Filter government / municipality sites
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if host.endswith(".gov") or host.endswith(".edu"):
        return True
    # Pattern: www.ci.cityname.state.us
    if re.search(r"\bci\.[a-z]+\.[a-z]{2}\.us$", host):
        return True
    if host.endswith(".us") and any(x in host for x in ("city.", "county.", "town.", ".ci.", ".co.")):
        return True
    # Filter paths that look like government file repos
    path = parsed.path.lower()
    if "/sites/default/files/" in path or "/fileattachments/" in path:
        return True
    d = _norm_domain(url)
    return any(d == b or d.endswith("." + b) for b in _BAD_DOMAINS)


def _clean_name(name: str) -> str:
    out = re.sub(r",?\s*\b(llc|pllc|inc|pa|ltd|corp|dba)\b", "", name, flags=re.I)
    out = re.sub(r",?\s*\b(d\.?c\.?|dpt|pt|lpt|mspt|mpt|dc|md)\b", "", out, flags=re.I)
    return re.sub(r"\s+", " ", out).strip().strip(",").strip()


_GENERIC_WORDS = frozenset({
    "chiropractic", "chiropractor", "therapy", "therapist", "physical",
    "center", "clinic", "group", "health", "wellness", "rehab", "rehabilitation",
    "sports", "spine", "family", "care", "institute", "services", "practice",
    "associates", "back", "joint", "pain", "injury", "advanced", "premier",
    "optimal", "integrated", "medical", "orthopedic", "acupuncture", "massage",
    "performance", "movement", "motion", "balance", "core", "active",
})


def _domain_has_name_hint(url: str, name: str, city: str) -> bool:
    """Light relevance check: domain/path should hint at name or city."""
    parsed = urlparse(url)
    domain = re.sub(r"^www\.", "", parsed.netloc.lower()).replace("-", "").replace(".", "")
    full_url_lower = url.lower()
    cleaned = _clean_name(name).lower()
    words = [w for w in re.split(r"\W+", cleaned) if len(w) >= 4 and w not in _GENERIC_WORDS]
    city_slug = re.sub(r"[^a-z0-9]", "", city.lower())
    if any(w in domain for w in words):
        return True
    if len(city_slug) >= 4 and city_slug in domain:
        return True
    # Also check URL path for last-name partial match
    for w in words:
        if w in full_url_lower:
            return True
    return False


def startpage_search(name: str, city: str, state: str, ptype: str) -> str:
    cleaned = _clean_name(name)
    if not cleaned:
        return ""
    query = f"{cleaned} {ptype} {city} {state}"
    url = f"https://www.startpage.com/sp/search?q={quote_plus(query)}&language=english"
    try:
        r = _SESSION.get(url, timeout=20)
        if r.status_code != 200:
            print(f"    [Startpage {r.status_code}] {name[:50]}", flush=True)
            return ""
        # Regex extraction: BeautifulSoup misses Startpage's encoded hrefs
        candidates = []
        for href in re.findall(r'href=["\']?(https?://[^"\'> &]+)', r.text):
            if "startpage.com" in href:
                continue
            href = href.replace("&amp;", "&")
            if not _is_bad(href):
                candidates.append(href)

        if not candidates:
            return ""

        # Prefer URLs with a name/city hint
        for href in candidates:
            if _domain_has_name_hint(href, name, city):
                return href

        # Fall back to first candidate if it looks like a real clinic site
        first = candidates[0]
        domain = _norm_domain(first)
        # Only take generic first result if it at least looks like a health/business site
        if re.search(r"(chiro|pt|therapy|health|wellness|spine|rehab|clinic|care)", domain, re.I):
            return first

        return ""
    except Exception as e:
        print(f"    [Startpage err] {name[:40]}: {e}", flush=True)
        return ""


def share_address_phone(records: list[dict]) -> int:
    addr_to_web: dict[tuple, str] = {}
    phone_to_web: dict[str, str] = {}
    for r in records:
        web = r.get("website", "").strip()
        if not web:
            continue
        addr = re.sub(r"\s+", " ", (r.get("street_address", "") or "").lower().strip())
        zip_ = (r.get("zip_code", "") or "")[:5]
        if addr and zip_ and len(addr) > 5:
            addr_to_web[(addr, zip_)] = web
        ph = re.sub(r"[^\d]", "", r.get("phone_number", "") or "")
        if len(ph) == 10:
            phone_to_web[ph] = web

    found = 0
    for r in records:
        if r.get("website", "").strip():
            continue
        addr = re.sub(r"\s+", " ", (r.get("street_address", "") or "").lower().strip())
        zip_ = (r.get("zip_code", "") or "")[:5]
        web = addr_to_web.get((addr, zip_), "")
        if not web:
            ph = re.sub(r"[^\d]", "", r.get("phone_number", "") or "")
            web = phone_to_web.get(ph, "") if len(ph) == 10 else ""
        if web:
            r["website"] = web
            found += 1
    return found


def read_csv(path: Path) -> list[dict]:
    return list(csv.DictReader(path.open()))


def write_csv(path: Path, records: list[dict]):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        w.writeheader()
        w.writerows(records)


def enrich_county(path: Path, county_idx: int, total_counties: int) -> dict:
    records = read_csv(path)
    before = sum(1 for r in records if r.get("website", "").strip())

    targets = [r for r in records if not r.get("website", "").strip() and _is_org(r.get("provider_name", ""))]
    slug = path.stem

    print(f"\n[{county_idx}/{total_counties}] {slug} — {len(records)} providers, {before} with website, {len(targets)} orgs need search", flush=True)

    found = 0
    for i, rec in enumerate(targets):
        site = startpage_search(
            rec.get("provider_name", ""),
            rec.get("city", ""),
            rec.get("state", "KS"),
            rec.get("provider_type", "").lower(),
        )
        if site:
            rec["website"] = site
            found += 1
            print(f"  [{i+1}/{len(targets)}] ✓ {rec['provider_name'][:45]} → {site}", flush=True)
        else:
            if (i + 1) % 10 == 0:
                print(f"  [{i+1}/{len(targets)}] {found} found so far", flush=True)
        time.sleep(15)

    # Share within county
    shared = share_address_phone(records)

    after = sum(1 for r in records if r.get("website", "").strip())
    write_csv(path, records)

    print(f"  {slug}: {before} → {after} websites (+{found} Startpage, +{shared} shared)", flush=True)
    return {"slug": slug, "before": before, "after": after, "found_startpage": found, "found_shared": shared}


def global_share_pass():
    """Cross-county sharing: same address/phone can share website across files."""
    print("\n=== Global address/phone sharing pass ===", flush=True)

    all_csvs = sorted(DATA_DIR.glob("providers-*-ks.csv"))
    all_records: list[dict] = []
    file_map: dict[str, list[dict]] = {}
    for p in all_csvs:
        rows = read_csv(p)
        all_records.extend(rows)
        file_map[str(p)] = rows

    total_shared = share_address_phone(all_records)
    print(f"  Global sharing: +{total_shared}", flush=True)

    # Write back
    offset = 0
    for p in all_csvs:
        rows = file_map[str(p)]
        write_csv(p, rows)

    return total_shared


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume", action="store_true",
                        help="Skip counties where no orgs need websites")
    parser.add_argument("--county", help="Process only this county slug")
    args = parser.parse_args()

    all_csvs = sorted(DATA_DIR.glob("providers-*-ks.csv"))

    if args.county:
        slug = args.county if args.county.endswith(".csv") else args.county
        slug = slug.replace(".csv", "")
        matches = [p for p in all_csvs if slug in p.stem]
        if not matches:
            print(f"No CSV found for {args.county}")
            sys.exit(1)
        all_csvs = matches

    total = len(all_csvs)
    print(f"Processing {total} county CSVs\n")

    stats = []
    for idx, csv_path in enumerate(all_csvs, 1):
        records = read_csv(csv_path)
        targets = [r for r in records if not r.get("website", "").strip() and _is_org(r.get("provider_name", ""))]
        if args.resume and not targets:
            continue
        stat = enrich_county(csv_path, idx, total)
        stats.append(stat)

    # Global sharing pass (propagate across county files)
    if not args.county:
        global_share_pass()

    # Summary
    print("\n=== Summary ===")
    total_startpage = sum(s["found_startpage"] for s in stats)
    total_shared = sum(s["found_shared"] for s in stats)
    total_gained = sum(s["after"] - s["before"] for s in stats)
    print(f"Startpage found:     {total_startpage}")
    print(f"Address/phone share: {total_shared}")
    print(f"Total websites gained: {total_gained}")

    # Overall coverage
    all_records: list[dict] = []
    for p in sorted(DATA_DIR.glob("providers-*-ks.csv")):
        all_records.extend(read_csv(p))
    with_web = sum(1 for r in all_records if r.get("website", "").strip())
    print(f"\nFinal coverage: {with_web}/{len(all_records)} ({100*with_web/len(all_records):.1f}%) have website")


if __name__ == "__main__":
    main()
