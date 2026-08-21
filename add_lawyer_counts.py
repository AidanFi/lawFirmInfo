#!/usr/bin/env python3
"""
Populate the number_of_lawyers column in a county CSV.

Sources (priority order):
  1. KS courts cache (attorney_count field) — KS counties only
  2. Website scraping — count attorney profile cards on team/attorneys pages
  3. Blank if neither yields reliable data

Usage: python3 add_lawyer_counts.py --county <slug>
       python3 add_lawyer_counts.py --all        (run on every county CSV)
"""

import argparse
import csv
import json
import re
import time
import warnings
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore", message="Unverified HTTPS request")

DATA_DIR = Path("app/county-data")
CACHE_DIR = Path("data/county")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

TEAM_PATHS = [
    "/attorneys", "/our-attorneys", "/our-team", "/team", "/lawyers",
    "/our-lawyers", "/meet-the-team", "/meet-our-attorneys", "/people",
    "/staff", "/about/attorneys", "/about/team", "/attorneys-staff",
    "/professionals", "/our-professionals",
]

# Patterns that indicate an attorney bio card / listing
ATTORNEY_INDICATORS = re.compile(
    r'\b(attorney|lawyer|counsel|esquire|esq\.?|j\.d\.|juris doctor|partner|associate)\b',
    re.IGNORECASE,
)


# ── KS courts cache lookup ────────────────────────────────────────────────────

def _load_ks_cache(slug: str) -> dict[str, int]:
    """Return name→attorney_count mapping from KS courts cache."""
    path = CACHE_DIR / f"{slug}_ks_courts_cache.json"
    if not path.exists():
        return {}
    data = json.loads(path.read_text())
    firms = data.get("firms", [])
    result: dict[str, int] = {}
    for firm in firms:
        name = str(firm.get("name", "")).strip("'\"").strip().lower()
        count_raw = str(firm.get("attorney_count", "")).strip("'\"").strip()
        try:
            count = int(count_raw)
            if count > 0:
                result[name] = count
        except (ValueError, TypeError):
            pass
    return result


STATEWIDE_CACHE_PATH = CACHE_DIR / "_statewide_lawyer_counts.json"


def _load_statewide_cache() -> dict[str, int]:
    if not STATEWIDE_CACHE_PATH.exists():
        return {}
    return json.loads(STATEWIDE_CACHE_PATH.read_text())


def _normalize_name(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(r"\b(law|firm|office|offices|group|llc|llp|pc|pa|pllc|plc|&|and|the|attorney|attorneys|at|of|lawyer|lawyers|chartered|chtd)\b", "", name)
    name = re.sub(r"[^a-z0-9]", "", name)
    return name


def _lookup_statewide_count(firm_name: str, city: str, statewide: dict[str, int]) -> int | None:
    key = _normalize_name(firm_name) + "|" + city.lower().strip()
    return statewide.get(key)


def _lookup_ks_count(firm_name: str, cache: dict[str, int]) -> int | None:
    key = firm_name.strip().lower()
    if key in cache:
        return cache[key]
    # Fuzzy: strip common suffixes
    for suffix in [", llc", ", lc", ", pc", ", p.c.", ", p.a.", ", pa",
                   " law", " law firm", " law office", " law offices",
                   " attorney at law", " attorneys at law", " chtd.", " chtd",
                   " chartered", " llp", " pllc"]:
        stripped = key.removesuffix(suffix)
        if stripped != key and stripped in cache:
            return cache[stripped]
    return None


# ── Website scraping ──────────────────────────────────────────────────────────

def _fetch(url: str, timeout: int = 8) -> BeautifulSoup | None:
    for verify in (True, False):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout, verify=verify,
                             allow_redirects=True)
            if r.status_code == 200 and "text/html" in r.headers.get("content-type", ""):
                return BeautifulSoup(r.text, "lxml")
        except Exception:
            pass
    return None


def _count_attorneys_on_page(soup: BeautifulSoup) -> int | None:
    """Try to count distinct attorney entries on a team/attorneys page."""
    # Strategy 1: count schema.org Person entries
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get("@type") == "LegalService":
                    employees = item.get("employee", [])
                    if employees:
                        return len(employees)
        except Exception:
            pass

    # Strategy 2: count h2/h3 headings that look like attorney names
    # (typically "First Last" or "First M. Last, J.D.")
    name_pattern = re.compile(r'^[A-Z][a-z]+ .{2,40}$')
    headings = soup.find_all(["h2", "h3", "h4"])
    attorney_headings = [h for h in headings if name_pattern.match(h.get_text(strip=True))
                         and ATTORNEY_INDICATORS.search(h.get_text() + " " +
                             (h.find_next_sibling() or h.parent or h).get_text())]
    if len(attorney_headings) >= 2:
        return len(attorney_headings)

    # Strategy 3: look for cards/divs with class hints
    card_selectors = [
        '[class*="attorney"]', '[class*="lawyer"]', '[class*="team-member"]',
        '[class*="person"]', '[class*="bio"]', '[class*="staff"]',
        '[class*="profile"]',
    ]
    for sel in card_selectors:
        cards = soup.select(sel)
        # Filter to cards that contain attorney-like text
        legit = [c for c in cards if ATTORNEY_INDICATORS.search(c.get_text())]
        if len(legit) >= 2:
            return len(legit)

    # Strategy 4: count headings with J.D., Esq., Attorney patterns
    all_text_blocks = soup.find_all(["h1", "h2", "h3", "h4", "p", "li"])
    jd_matches = set()
    for el in all_text_blocks:
        txt = el.get_text(strip=True)
        if re.search(r'\b(j\.d\.|esq\.?|attorney at law|bar number)\b', txt, re.IGNORECASE):
            jd_matches.add(txt[:60])
    if len(jd_matches) >= 2:
        return len(jd_matches)

    return None


def _scrape_lawyer_count(website: str) -> int | None:
    base = website.rstrip("/")

    # Try homepage first (solo practitioners often only have 1 page)
    soup = _fetch(base)
    if soup:
        count = _count_attorneys_on_page(soup)
        if count is not None:
            return count

    # Try team/attorney sub-pages
    for path in TEAM_PATHS:
        time.sleep(0.3)
        soup = _fetch(base + path)
        if soup:
            count = _count_attorneys_on_page(soup)
            if count is not None:
                return count

    return None


# ── Main ─────────────────────────────────────────────────────────────────────

def process_county(slug: str, ks_cache: dict[str, int], statewide_cache: dict[str, int] | None = None) -> None:
    csv_path = DATA_DIR / f"{slug}.csv"
    if not csv_path.exists():
        print(f"  [skip] {slug}: CSV not found")
        return

    rows = list(csv.DictReader(csv_path.open()))
    if not rows:
        return

    # Add column if missing
    fieldnames = list(rows[0].keys())
    if "number_of_lawyers" not in fieldnames:
        fieldnames.append("number_of_lawyers")
        for r in rows:
            r.setdefault("number_of_lawyers", "")

    cache_hits = 0
    web_hits = 0
    skipped = 0

    for i, row in enumerate(rows, 1):
        # Skip if already populated
        if row.get("number_of_lawyers", "").strip():
            skipped += 1
            continue

        count = None

        # 1. KS courts cache (per-county)
        if ks_cache:
            count = _lookup_ks_count(row.get("law_firm_name", ""), ks_cache)
            if count is not None:
                cache_hits += 1

        # 1b. KS courts cache (statewide, name+city keyed) fallback
        if count is None and statewide_cache:
            count = _lookup_statewide_count(row.get("law_firm_name", ""), row.get("city", ""), statewide_cache)
            if count is not None:
                cache_hits += 1

        # 2. Website scraping
        if count is None and row.get("website", "").strip():
            count = _scrape_lawyer_count(row["website"].strip())
            if count is not None:
                web_hits += 1
            time.sleep(0.4)

        row["number_of_lawyers"] = str(count) if count is not None else ""

        if i % 50 == 0:
            filled = sum(1 for r in rows[:i] if r.get("number_of_lawyers", "").strip())
            print(f"  Progress: {i}/{len(rows)} | filled={filled} (cache={cache_hits}, web={web_hits})")

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    filled = sum(1 for r in rows if r.get("number_of_lawyers", "").strip())
    print(f"  Done: {filled}/{len(rows)} filled (cache={cache_hits}, web={web_hits}, pre-filled={skipped})")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--county", help="County slug (e.g. johnson-county-ks)")
    parser.add_argument("--all", action="store_true", help="Run on all county CSVs")
    args = parser.parse_args()

    slugs = []
    if args.all:
        slugs = [p.stem for p in sorted(DATA_DIR.glob("*.csv"))]
    elif args.county:
        slugs = [args.county]
    else:
        parser.error("Provide --county <slug> or --all")

    statewide_cache = _load_statewide_cache()
    for slug in slugs:
        print(f"\n[{slug}]")
        ks_cache = _load_ks_cache(slug)
        if ks_cache:
            print(f"  KS courts cache: {len(ks_cache)} firms")
        process_county(slug, ks_cache, statewide_cache)


if __name__ == "__main__":
    main()
