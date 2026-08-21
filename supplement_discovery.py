#!/usr/bin/env python3
"""
Supplemental law firm discovery for underserved counties.
Scrapes Justia, Avvo, Martindale, SuperLawyers for firms not already in CSVs.
Adds new firms to the existing CSV.

Usage: python3 supplement_discovery.py --county jackson-county-mo
"""

import csv
import json
import re
import sys
import time
import warnings
from pathlib import Path
from urllib.parse import urljoin, urlparse

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
# Config per county
# ---------------------------------------------------------------------------

COUNTY_CONFIGS = {
    "jackson-county-mo": {
        "county": "Jackson",
        "state": "MO",
        "msa": "Kansas City",
        "primary_city": "Kansas City",
        "cities": ["Kansas City", "Independence", "Blue Springs", "Lee's Summit",
                   "Raytown", "Grandview", "Sugar Creek", "Grain Valley"],
        "zip_codes": {
            "64101","64102","64103","64104","64105","64106","64107","64108",
            "64109","64110","64111","64112","64113","64114","64120",
            "64123","64124","64125","64126","64127","64128","64129",
            "64130","64131","64132","64133","64134","64136","64137",
            "64138","64139","64145","64146","64147","64148","64149",
            "64050","64052","64053","64054","64055","64056","64057","64058",
            "64013","64014","64015","64016","64063","64064","64065",
            "64082","64086","64029","64030","64075","64070",
        },
        "valid_area_codes": {"816", "913", "785", "660"},
    },
    "greene-county-mo": {
        "county": "Greene",
        "state": "MO",
        "msa": "Springfield",
        "primary_city": "Springfield",
        "cities": ["Springfield", "Republic", "Battlefield", "Strafford", "Willard"],
        "zip_codes": {
            "65801","65802","65803","65804","65806","65807","65809","65810",
            "65738","65619","65757","65781","65604","65648","65612",
        },
        "valid_area_codes": {"417", "573"},
    },
    "st-charles-county-mo": {
        "county": "St. Charles",
        "state": "MO",
        "msa": "St. Louis",
        "primary_city": "St. Charles",
        "cities": ["O'Fallon", "St. Peters", "St. Charles", "Wentzville",
                   "Lake Saint Louis", "Cottleville"],
        "zip_codes": {
            "63301","63303","63304","63332","63338","63341","63348",
            "63362","63363","63366","63367","63368","63373","63376","63385",
        },
        "valid_area_codes": {"636", "314"},
    },
    # ── Kansas counties ────────────────────────────────────────────────────────
    "johnson-county-ks": {
        "county": "Johnson",
        "state": "KS",
        "msa": "Kansas City",
        "primary_city": "Overland Park",
        "cities": [
            "Overland Park", "Olathe", "Shawnee", "Lenexa", "Leawood",
            "Prairie Village", "Merriam", "Mission", "Gardner", "Spring Hill",
            "De Soto", "Roeland Park", "Fairway", "Westwood", "Edgerton",
        ],
        "valid_area_codes": {"913", "816"},
    },
    "wyandotte-county-ks": {
        "county": "Wyandotte",
        "state": "KS",
        "msa": "Kansas City",
        "primary_city": "Kansas City",
        "cities": ["Kansas City", "Bonner Springs", "Edwardsville"],
        "valid_area_codes": {"913", "816"},
    },
    "leavenworth-county-ks": {
        "county": "Leavenworth",
        "state": "KS",
        "msa": "Kansas City",
        "primary_city": "Leavenworth",
        "cities": ["Leavenworth", "Lansing", "Basehor", "Tonganoxie", "Linwood", "Easton"],
        "valid_area_codes": {"913", "785"},
    },
    "miami-county-ks": {
        "county": "Miami",
        "state": "KS",
        "msa": "Kansas City",
        "primary_city": "Paola",
        "cities": ["Paola", "Osawatomie", "Louisburg", "Fontana", "Spring Hill"],
        "valid_area_codes": {"913", "785"},
    },
    "linn-county-ks": {
        "county": "Linn",
        "state": "KS",
        "msa": "Kansas City",
        "primary_city": "Pleasanton",
        "cities": ["Pleasanton", "La Cygne", "Mound City", "Prescott", "Blue Mound"],
        "valid_area_codes": {"913", "785"},
    },
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_LAW_INDICATORS = re.compile(
    r'\b(?:law|legal|attorney|attorneys|lawyer|lawyers|counsel|llp|pllc|'
    r'p\.c\.|p\.a\.|litigation|criminal|injury|divorce|bankruptcy|'
    r'immigration|probate|defender|defense|mediat|esq|juris)\b',
    re.IGNORECASE,
)

_PHONE_RE = re.compile(r'\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4}')
_EMAIL_RE = re.compile(r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,6}\b')

PRIORITY_SCORES = {
    "Criminal Defense": 5, "DUI": 5, "Personal Injury": 5,
    "Medical Malpractice": 5, "Workers' Compensation": 5,
    "Sexual Assault": 4, "Family Law": 4, "General Practice": 4,
    "Employment Law": 3, "Civil Litigation": 3,
    "Estate Planning": 2, "Bankruptcy": 2,
    "Real Estate": 2, "Business Law": 2,
    "Immigration": 2, "Tax Law": 2,
    "Social Security Disability": 2, "Intellectual Property": 1,
}


def norm_name(s: str) -> str:
    return re.sub(r'[^a-z0-9]', '', s.lower())


def norm_domain(url: str) -> str:
    if not url:
        return ""
    try:
        return urlparse(url).netloc.lower().lstrip("www.")
    except Exception:
        return ""


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


def load_existing(slug: str) -> tuple[set, set]:
    """Returns (existing_name_keys, existing_domains)."""
    path = DATA_DIR / f"{slug}.csv"
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    names = {norm_name(r["law_firm_name"]) for r in rows}
    domains = {norm_domain(r["website"]) for r in rows if r["website"]}
    return names, domains


def is_new(name: str, website: str, existing_names: set, existing_domains: set) -> bool:
    key = norm_name(name)
    if key in existing_names:
        return False
    # Check partial name match (first 15 chars)
    if len(key) >= 10:
        for ex in existing_names:
            if key[:15] in ex or ex[:15] in key:
                return False
    if website:
        domain = norm_domain(website)
        if domain and domain in existing_domains:
            return False
    return True


# ---------------------------------------------------------------------------
# Justia scraper
# ---------------------------------------------------------------------------

def scrape_justia(city: str, state: str = "missouri") -> list[dict]:
    """Scrape Justia lawyer directory for a city."""
    city_slug = city.lower().replace(" ", "-").replace("'", "")
    firms = []

    for page in range(1, 8):  # up to 7 pages
        if page == 1:
            url = f"https://www.justia.com/lawyers/{state}/{city_slug}"
        else:
            url = f"https://www.justia.com/lawyers/{state}/{city_slug}?page={page}"

        resp = _fetch(url)
        if not resp:
            break

        soup = BeautifulSoup(resp.text, "html.parser")

        # Find lawyer/firm listings
        found_on_page = 0
        for card in soup.find_all(["div", "li"], class_=re.compile(r'lawyer|attorney|listing|result', re.I)):
            name_tag = card.find(["h2", "h3", "h4", "a"], class_=re.compile(r'name|title', re.I))
            if not name_tag:
                name_tag = card.find(["h2", "h3", "h4"])
            if not name_tag:
                continue
            name = name_tag.get_text(strip=True)
            if not name or len(name) < 3:
                continue

            # Get website link
            website = ""
            for a in card.find_all("a", href=True):
                href = a["href"]
                if href.startswith("http") and "justia.com" not in href and "avvo.com" not in href:
                    website = href
                    break

            phone = ""
            phone_m = _PHONE_RE.search(card.get_text())
            if phone_m:
                phone = phone_m.group()

            practice = card.get_text(separator=" ", strip=True)

            firms.append({
                "name": name,
                "website": website,
                "phone": phone,
                "city": city,
                "practice_text": practice[:500],
            })
            found_on_page += 1

        if found_on_page == 0:
            break
        time.sleep(1.5)

    return firms


# ---------------------------------------------------------------------------
# Avvo scraper
# ---------------------------------------------------------------------------

def scrape_avvo(city: str, state: str = "mo") -> list[dict]:
    """Scrape Avvo all-lawyers page for a city."""
    city_slug = city.lower().replace(" ", "-").replace("'", "")
    firms = []

    for page in range(1, 8):
        if page == 1:
            url = f"https://www.avvo.com/all-lawyers/{city_slug}-{state}.html"
        else:
            url = f"https://www.avvo.com/all-lawyers/{city_slug}-{state}/{page}.html"

        resp = _fetch(url)
        if not resp:
            break

        soup = BeautifulSoup(resp.text, "html.parser")

        found_on_page = 0
        for card in soup.find_all(["div"], class_=re.compile(r'attorney|lawyer|result|listing', re.I)):
            name_tag = card.find(["h2", "h3", "h4", "span"], class_=re.compile(r'name', re.I))
            if not name_tag:
                continue
            name = name_tag.get_text(strip=True)
            if not name or len(name) < 3:
                continue

            website = ""
            for a in card.find_all("a", href=True):
                href = a["href"]
                if (href.startswith("http") and
                        "avvo.com" not in href and
                        "google.com" not in href):
                    website = href
                    break

            phone = ""
            phone_m = _PHONE_RE.search(card.get_text())
            if phone_m:
                phone = phone_m.group()

            firms.append({
                "name": name,
                "website": website,
                "phone": phone,
                "city": city,
                "practice_text": card.get_text(separator=" ", strip=True)[:500],
            })
            found_on_page += 1

        if found_on_page == 0:
            break
        time.sleep(1.5)

    return firms


# ---------------------------------------------------------------------------
# SuperLawyers scraper
# ---------------------------------------------------------------------------

def scrape_superlawyers(city: str, state: str = "missouri") -> list[dict]:
    """Scrape SuperLawyers directory."""
    city_slug = city.lower().replace(" ", "-").replace("'", "")
    firms = []
    url = f"https://www.superlawyers.com/{state}/city/{city_slug}/"

    resp = _fetch(url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    for card in soup.find_all(["div", "li"], class_=re.compile(r'result|listing|attorney|lawyer', re.I)):
        name_tag = card.find(["h2", "h3", "h4", "a"], class_=re.compile(r'name|title', re.I))
        if not name_tag:
            continue
        name = name_tag.get_text(strip=True)
        if not name or len(name) < 3:
            continue

        website = ""
        for a in card.find_all("a", href=True):
            href = a["href"]
            if href.startswith("http") and "superlawyers.com" not in href:
                website = href
                break

        firms.append({
            "name": name, "website": website, "phone": "",
            "city": city, "practice_text": "",
        })

    time.sleep(1.5)
    return firms


# ---------------------------------------------------------------------------
# Practice area from text
# ---------------------------------------------------------------------------

PRACTICE_KEYWORDS = {
    "Personal Injury": ["personal injury", "car accident", "auto accident", "wrongful death"],
    "Family Law": ["family law", "divorce", "child custody", "child support"],
    "Criminal Defense": ["criminal defense", "dui", "dwi", "criminal law", "felony"],
    "Estate Planning": ["estate planning", "wills", "trusts", "probate", "elder law"],
    "Workers' Compensation": ["workers comp", "workers' compensation", "work injury"],
    "Bankruptcy": ["bankruptcy", "chapter 7", "chapter 13", "debt relief"],
    "Business Law": ["business law", "corporate law", "business attorney"],
    "Real Estate": ["real estate law", "property law", "real estate attorney"],
    "Immigration": ["immigration", "visa", "green card", "citizenship"],
    "Employment Law": ["employment law", "wrongful termination", "discrimination"],
    "Medical Malpractice": ["medical malpractice", "medical negligence"],
    "Social Security Disability": ["social security disability", "ssdi"],
    "Tax Law": ["tax law", "tax attorney", "irs"],
}


def guess_practice(text: str) -> str:
    lower = text.lower()
    for area, kws in PRACTICE_KEYWORDS.items():
        if any(kw in lower for kw in kws):
            return area
    return "General"


# ---------------------------------------------------------------------------
# Build CSV row from discovery
# ---------------------------------------------------------------------------

def make_row(firm: dict, county_cfg: dict) -> dict:
    name = firm["name"].strip()
    practice = guess_practice(firm.get("practice_text", ""))
    priority = PRIORITY_SCORES.get(practice, 4)

    # Clean phone
    phone = firm.get("phone", "").strip()
    if phone and not phone.startswith("("):
        m = _PHONE_RE.search(phone)
        phone = m.group() if m else phone

    return {
        "law_firm_name": name,
        "website": firm.get("website", ""),
        "google_business_profile": "",
        "legal_directory_listing": "",
        "city": firm.get("city", county_cfg["primary_city"]),
        "state": county_cfg["state"],
        "county": county_cfg["county"],
        "phone_number": phone,
        "email": "",
        "practice_area": practice,
        "street_address": "",
        "zip_code": "",
        "msa": county_cfg["msa"],
        "priority": str(priority),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def supplement(slug: str) -> int:
    cfg = COUNTY_CONFIGS[slug]
    existing_names, existing_domains = load_existing(slug)
    print(f"\n[{slug}] Existing: {len(existing_names)} firms, {len(existing_domains)} domains")

    all_discovered = []

    state_long = "missouri" if cfg["state"] == "MO" else "kansas"

    # Justia - all cities
    for city in cfg["cities"]:
        print(f"  Justia: {city}...", end=" ", flush=True)
        found = scrape_justia(city, state_long)
        print(f"{len(found)} entries")
        all_discovered.extend(found)
        time.sleep(1.0)

    # Avvo - all cities
    for city in cfg["cities"]:
        print(f"  Avvo: {city}...", end=" ", flush=True)
        found = scrape_avvo(city, cfg["state"].lower())
        print(f"{len(found)} entries")
        all_discovered.extend(found)
        time.sleep(1.0)

    # SuperLawyers - primary city
    print(f"  SuperLawyers: {cfg['primary_city']}...", end=" ", flush=True)
    found = scrape_superlawyers(cfg["primary_city"], state_long)
    print(f"{len(found)} entries")
    all_discovered.extend(found)

    print(f"\n  Total raw discovered: {len(all_discovered)}")

    # Filter: must have law indicator in name or practice area
    law_firms = []
    for f in all_discovered:
        name = f["name"].strip()
        if not name or len(name) < 4:
            continue
        if not _LAW_INDICATORS.search(name):
            practice = guess_practice(f.get("practice_text", ""))
            if practice == "General" and not _LAW_INDICATORS.search(f.get("practice_text", "")):
                continue
        law_firms.append(f)

    print(f"  Law firms after filter: {len(law_firms)}")

    # Deduplicate against existing
    new_firms = []
    seen_in_batch = set()
    for f in law_firms:
        name = f["name"].strip()
        website = f.get("website", "")
        key = norm_name(name)
        if key in seen_in_batch:
            continue
        if is_new(name, website, existing_names, existing_domains):
            new_firms.append(f)
            seen_in_batch.add(key)
            existing_names.add(key)
            if website:
                existing_domains.add(norm_domain(website))

    print(f"  Genuinely new firms: {len(new_firms)}")

    if not new_firms:
        print("  Nothing new to add.")
        return 0

    # Load existing CSV and append
    path = DATA_DIR / f"{slug}.csv"
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    fieldnames = list(rows[0].keys())

    new_rows = [make_row(f, cfg) for f in new_firms]

    # Only add rows where the name has a law indicator (final safety check)
    final_rows = [r for r in new_rows if _LAW_INDICATORS.search(r["law_firm_name"])]
    print(f"  Adding {len(final_rows)} rows after final law-indicator check")

    if final_rows:
        rows.extend(final_rows)
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)

        # Update manifest
        mpath = DATA_DIR / "manifest.json"
        manifest = json.loads(mpath.read_text())
        from datetime import date
        today = date.today().isoformat()
        for c in manifest["counties"]:
            if c["slug"] == slug:
                c["firm_count"] = len(rows)
                c["last_updated"] = today
                break
        mpath.write_text(json.dumps(manifest, indent=2) + "\n")
        print(f"  {slug} updated: {len(rows) - len(final_rows)} → {len(rows)} firms")

    return len(final_rows)


if __name__ == "__main__":
    target = None
    if "--county" in sys.argv:
        idx = sys.argv.index("--county")
        if idx + 1 < len(sys.argv):
            target = sys.argv[idx + 1]

    slugs = [target] if target else list(COUNTY_CONFIGS.keys())
    total_added = 0
    for slug in slugs:
        added = supplement(slug)
        total_added += added

    print(f"\nTotal new firms added: {total_added}")
