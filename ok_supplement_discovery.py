#!/usr/bin/env python3
"""
Supplemental law firm discovery for all 14 Oklahoma county CSVs.
Scrapes Justia (multi-page), Avvo, FindLaw, and SuperLawyers for each county
to find firms not already in the data.

Usage: python3 ok_supplement_discovery.py [slug ...]
       python3 ok_supplement_discovery.py          # all 14 OK counties
"""
import csv
import re
import sys
import time
import warnings
from pathlib import Path
from urllib.parse import urlparse, urljoin

import requests
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

# ---------------------------------------------------------------------------
# County configs — all 14 OK counties
# ---------------------------------------------------------------------------

COUNTY_CONFIGS = {
    "oklahoma-county-ok": {
        "county": "Oklahoma",
        "state": "OK",
        "state_name": "oklahoma",
        "msa": "Oklahoma City",
        "cities": ["Oklahoma City", "Edmond", "Moore", "Midwest City", "Del City",
                   "Bethany", "Warr Acres", "Nichols Hills", "The Village", "Choctaw"],
    },
    "tulsa-county-ok": {
        "county": "Tulsa",
        "state": "OK",
        "state_name": "oklahoma",
        "msa": "Tulsa",
        "cities": ["Tulsa", "Broken Arrow", "Owasso", "Sand Springs", "Jenks",
                   "Bixby", "Collinsville", "Glenpool", "Skiatook", "Catoosa"],
    },
    "cleveland-county-ok": {
        "county": "Cleveland",
        "state": "OK",
        "state_name": "oklahoma",
        "msa": "Oklahoma City",
        "cities": ["Norman", "Moore", "Noble", "Lexington", "Goldsby"],
    },
    "canadian-county-ok": {
        "county": "Canadian",
        "state": "OK",
        "state_name": "oklahoma",
        "msa": "Oklahoma City",
        "cities": ["Yukon", "Mustang", "El Reno", "Piedmont", "Tuttle", "Weatherford"],
    },
    "rogers-county-ok": {
        "county": "Rogers",
        "state": "OK",
        "state_name": "oklahoma",
        "msa": "Tulsa",
        "cities": ["Claremore", "Inola", "Chelsea", "Oologah", "Foyil"],
    },
    "wagoner-county-ok": {
        "county": "Wagoner",
        "state": "OK",
        "state_name": "oklahoma",
        "msa": "Tulsa",
        "cities": ["Wagoner", "Coweta", "Porter", "Okay"],
    },
    "grady-county-ok": {
        "county": "Grady",
        "state": "OK",
        "state_name": "oklahoma",
        "msa": "Oklahoma City",
        "cities": ["Chickasha", "Blanchard", "Ninnekah", "Rush Springs", "Minco"],
    },
    "pottawatomie-county-ok": {
        "county": "Pottawatomie",
        "state": "OK",
        "state_name": "oklahoma",
        "msa": "Oklahoma City",
        "cities": ["Shawnee", "Tecumseh", "McLoud", "Meeker", "Prague"],
    },
    "creek-county-ok": {
        "county": "Creek",
        "state": "OK",
        "state_name": "oklahoma",
        "msa": "Tulsa",
        "cities": ["Sapulpa", "Bristow", "Drumright", "Mannford", "Kiefer"],
    },
    "mcclain-county-ok": {
        "county": "McClain",
        "state": "OK",
        "state_name": "oklahoma",
        "msa": "Oklahoma City",
        "cities": ["Purcell", "Newcastle", "Lindsay", "Washington", "Blanchard"],
    },
    "logan-county-ok": {
        "county": "Logan",
        "state": "OK",
        "state_name": "oklahoma",
        "msa": "Oklahoma City",
        "cities": ["Guthrie", "Crescent", "Cashion", "Coyle"],
    },
    "washington-county-ok": {
        "county": "Washington",
        "state": "OK",
        "state_name": "oklahoma",
        "msa": "Tulsa",
        "cities": ["Bartlesville", "Dewey", "Copan", "Ochelata"],
    },
    "okmulgee-county-ok": {
        "county": "Okmulgee",
        "state": "OK",
        "state_name": "oklahoma",
        "msa": "Tulsa",
        "cities": ["Okmulgee", "Henryetta", "Beggs", "Morris"],
    },
    "osage-county-ok": {
        "county": "Osage",
        "state": "OK",
        "state_name": "oklahoma",
        "msa": "Tulsa",
        "cities": ["Pawhuska", "Hominy", "Fairfax", "Barnsdall"],
    },
}

PRIORITY_SCORES = {
    "Criminal Defense": 5, "DUI": 5, "Personal Injury": 5,
    "Medical Malpractice": 5, "Workers Compensation": 5,
    "Family Law": 4, "General Practice": 4, "Civil Litigation": 3,
    "Employment Law": 3, "Estate Planning": 2, "Bankruptcy": 2,
    "Real Estate": 2, "Business Law": 2, "Immigration": 2,
    "Tax Law": 2, "Social Security Disability": 2, "General": 1,
}

_PHONE_RE = re.compile(r'\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4}')

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _norm(s: str) -> str:
    return re.sub(r'[^a-z0-9]', '', s.lower())


def _fetch(url: str, timeout: int = 10) -> requests.Response | None:
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


def _norm_domain(url: str) -> str:
    if not url:
        return ""
    try:
        return urlparse(url).netloc.lower().lstrip("www.")
    except Exception:
        return ""


def _load_existing(slug: str) -> tuple[set, set]:
    path = DATA_DIR / f"{slug}.csv"
    rows = list(csv.DictReader(path.open()))
    names = {_norm(r["law_firm_name"]) for r in rows}
    domains = {_norm_domain(r["website"]) for r in rows if r.get("website")}
    return names, domains


def _is_new(name: str, website: str, existing_names: set, existing_domains: set) -> bool:
    key = _norm(name)
    if key in existing_names:
        return False
    if len(key) >= 10:
        for ex in existing_names:
            if key[:15] in ex or ex[:15] in key:
                return False
    if website:
        domain = _norm_domain(website)
        if domain and domain in existing_domains:
            return False
    return True


def _make_row(name: str, city: str, cfg: dict, website: str = "", phone: str = "",
              practice: str = "General", source: str = "") -> dict:
    priority = str(PRIORITY_SCORES.get(practice, 1))
    return {
        "law_firm_name": name,
        "website": website,
        "google_business_profile": "",
        "legal_directory_listing": "",
        "city": city,
        "state": cfg["state"],
        "county": cfg["county"],
        "phone_number": phone,
        "email": "",
        "practice_area": practice,
        "street_address": "",
        "zip_code": "",
        "msa": cfg.get("msa", ""),
        "priority": priority,
        "number_of_lawyers": "",
    }


# ---------------------------------------------------------------------------
# Justia scraper (multi-page)
# ---------------------------------------------------------------------------

def scrape_justia(city: str, state_name: str) -> list[dict]:
    city_slug = city.lower().replace(" ", "-").replace("'", "")
    firms = []
    for page in range(1, 6):
        if page == 1:
            url = f"https://www.justia.com/lawyers/{state_name}/{city_slug}"
        else:
            url = f"https://www.justia.com/lawyers/{state_name}/{city_slug}?page={page}"
        resp = _fetch(url)
        if not resp:
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        found_on_page = 0
        for card in soup.find_all(["div", "li"], class_=re.compile(r'lawyer|attorney|listing|result', re.I)):
            name_tag = card.find(["h2", "h3", "h4", "a"])
            if not name_tag:
                continue
            name = name_tag.get_text(strip=True)
            if not name or len(name) < 4:
                continue
            website = ""
            phone = ""
            for a in card.find_all("a", href=True):
                href = a["href"]
                if href.startswith("http") and "justia.com" not in href:
                    website = href
                    break
            phone_m = _PHONE_RE.search(card.get_text())
            if phone_m:
                phone = phone_m.group()
            firms.append({"name": name, "website": website, "phone": phone})
            found_on_page += 1
        if found_on_page == 0:
            break
        time.sleep(1.0)
    return firms


# ---------------------------------------------------------------------------
# Avvo scraper
# ---------------------------------------------------------------------------

def scrape_avvo(city: str, state: str) -> list[dict]:
    city_slug = city.lower().replace(" ", "-").replace("'", "")
    url = f"https://www.avvo.com/all-lawyers/{city_slug}-{state.lower()}.html"
    resp = _fetch(url)
    if not resp:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    firms = []
    for card in soup.find_all(["div", "li"], class_=re.compile(r'lawyer|attorney|result|listing', re.I)):
        name_tag = card.find(["h2", "h3", "h4", "a"])
        if not name_tag:
            continue
        name = name_tag.get_text(strip=True)
        if not name or len(name) < 4:
            continue
        website = ""
        for a in card.find_all("a", href=True):
            href = a["href"]
            if href.startswith("http") and "avvo.com" not in href:
                website = href
                break
        phone_m = _PHONE_RE.search(card.get_text())
        phone = phone_m.group() if phone_m else ""
        firms.append({"name": name, "website": website, "phone": phone})
    time.sleep(1.0)
    return firms


# ---------------------------------------------------------------------------
# FindLaw scraper
# ---------------------------------------------------------------------------

def scrape_findlaw(city: str, state: str) -> list[dict]:
    city_slug = city.lower().replace(" ", "+")
    url = f"https://lawyers.findlaw.com/lawyer/firm/practice/{state}/{city_slug}"
    resp = _fetch(url)
    if not resp:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    firms = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/profile/" not in href and "/firm/" not in href:
            continue
        name = a.get_text(strip=True)
        if not name or len(name) < 4:
            continue
        firms.append({"name": name, "website": "", "phone": ""})
    time.sleep(1.0)
    return firms


# ---------------------------------------------------------------------------
# SuperLawyers scraper
# ---------------------------------------------------------------------------

def scrape_superlawyers(city: str, state_name: str) -> list[dict]:
    city_slug = city.lower().replace(" ", "-")
    url = f"https://www.superlawyers.com/{state_name}/city/{city_slug}/"
    resp = _fetch(url)
    if not resp:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    firms = []
    for card in soup.find_all(["div", "li"], class_=re.compile(r'lawyer|attorney|profile|result', re.I)):
        name_tag = card.find(["h2", "h3", "h4", "a"])
        if not name_tag:
            continue
        name = name_tag.get_text(strip=True)
        if not name or len(name) < 4:
            continue
        website = ""
        for a in card.find_all("a", href=True):
            href = a["href"]
            if href.startswith("http") and "superlawyers.com" not in href:
                website = href
                break
        phone_m = _PHONE_RE.search(card.get_text())
        phone = phone_m.group() if phone_m else ""
        firms.append({"name": name, "website": website, "phone": phone})
    time.sleep(1.0)
    return firms


# ---------------------------------------------------------------------------
# Main per-county discovery
# ---------------------------------------------------------------------------

def discover_county(slug: str) -> int:
    cfg = COUNTY_CONFIGS.get(slug)
    if not cfg:
        print(f"  SKIP: no config for {slug}")
        return 0

    existing_names, existing_domains = _load_existing(slug)
    p = DATA_DIR / f"{slug}.csv"
    rows = list(csv.DictReader(p.open()))
    fieldnames = list(rows[0].keys())

    new_firms = []

    for city in cfg["cities"]:
        print(f"  {slug}/{city}:", end="", flush=True)
        candidates = []

        # Justia
        j = scrape_justia(city, cfg["state_name"])
        candidates.extend(j)

        # Avvo
        a = scrape_avvo(city, cfg["state"])
        candidates.extend(a)

        # FindLaw
        f = scrape_findlaw(city, cfg["state"])
        candidates.extend(f)

        # SuperLawyers
        s = scrape_superlawyers(city, cfg["state_name"])
        candidates.extend(s)

        added = 0
        for c in candidates:
            name = c.get("name", "").strip()
            website = c.get("website", "").strip()
            phone = c.get("phone", "").strip()

            if not name or len(name) < 4:
                continue
            if re.search(r'\b(justia|avvo|findlaw|superlawyers)\b', name, re.I):
                continue
            # Filter directory navigation text that scrapes as firm names
            if re.match(
                r'^(browse\s+law\s+firms?|by\s+lawyer\s+profiles?|practice\s+areas?|'
                r'sponsored\s+listings?|contact\s+us|related\s+searches?|'
                r'find\s+a\s+lawyer|view\s+profile|learn\s+more|advertisement|'
                r'attorney\s+search|law\s+firm\s+directory)',
                name, re.I,
            ):
                continue
            # Filter keyword-stuffed ad names (city + practice area + lawyer/attorney)
            if re.match(
                r'^(oklahoma\s+city|tulsa|broken\s+arrow|edmond|norman|shawnee|'
                r'yukon|mustang|el\s+reno|claremore|wagoner|chickasha|guthrie|'
                r'bartlesville|muskogee|stillwater)\s+'
                r'(car\s+accident|dui|dwi|bankruptcy|family\s+law|personal\s+injury|'
                r'criminal\s+defense|divorce|probate|estate\s+planning)\s+'
                r'(lawyer|attorney|attorneys?)$',
                name, re.I,
            ):
                continue
            if not _is_new(name, website, existing_names, existing_domains):
                continue

            # Mark as new so we don't add it twice from different sources
            existing_names.add(_norm(name))
            if website:
                existing_domains.add(_norm_domain(website))

            row = _make_row(name, city, cfg, website=website, phone=phone,
                            source=slug)
            new_firms.append(row)
            added += 1

        print(f" +{added}")

    if new_firms:
        all_rows = rows + new_firms
        all_rows.sort(key=lambda r: (r.get("city", ""), r.get("law_firm_name", "")))
        with p.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(all_rows)
        print(f"  -> Added {len(new_firms)} new firms to {slug}")

    return len(new_firms)


if __name__ == "__main__":
    if sys.argv[1:]:
        slugs = sys.argv[1:]
    else:
        slugs = list(COUNTY_CONFIGS.keys())

    print(f"Supplemental discovery for {len(slugs)} OK county CSV(s)...\n")
    total_new = 0
    for slug in slugs:
        new = discover_county(slug)
        total_new += new

    print(f"\nTotal new firms discovered: {total_new}")
