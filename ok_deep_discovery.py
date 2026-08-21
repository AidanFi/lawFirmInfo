#!/usr/bin/env python3
"""
Deep multi-source discovery for all 14 OK county CSVs.

Sources:
  1. Justia directory pages (JSON-LD) → profile pages (firm name + website)
  2. SuperLawyers directory
  3. FindLaw directory
  4. Avvo directory

Usage:
  python3 ok_deep_discovery.py              # all 14 counties
  python3 ok_deep_discovery.py tulsa-county-ok oklahoma-county-ok
"""
import csv
import json
import re
import sys
import time
import warnings
from pathlib import Path
from urllib.parse import urlparse

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

_PHONE_RE = re.compile(r"\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4}")
_OK_AREA = re.compile(r"^(\(?)(405|918|580)\)?")
_TOLLFREE = re.compile(r"^(\(?)(800|833|844|855|866|877|888)\)?")

_DIRECTORY_DOMAINS = frozenset({
    "justia.com", "lawyers.justia.com", "avvo.com", "findlaw.com",
    "martindale.com", "lawyers.com", "superlawyers.com", "nolo.com",
    "lawinfo.com", "hg.org", "lawyer.com", "bestlawyers.com", "usnews.com",
    "thumbtack.com", "facebook.com", "linkedin.com", "twitter.com",
    "instagram.com", "bbb.org", "google.com", "bing.com", "manta.com",
    "okbar.org", "ams.okbar.org", "superpages.com", "whitepages.com",
    "yellowpages.com", "yelp.com", "trellis.law", "chamberofcommerce.com",
    "birdeye.com", "attorneyslisted.com", "lawyerdb.org",
})

_ARTIFACT_RE = re.compile(
    r"^(browse\s+law\s+firms?|by\s+lawyer\s+profiles?|practice\s+areas?|"
    r"sponsored\s+listings?|contact\s+us|related\s+searches?|"
    r"find\s+a\s+lawyer|view\s+profile|advertisement|attorney\s+search|"
    r"law\s+firm\s+directory)",
    re.I,
)

_NON_LAW_RE = re.compile(
    r"\b(restaurant|cafe|diner|steak|ribs|bar\s+&\s+grill|auction\s+service|"
    r"construction|plumbing|electric|towing|auto\s+repair|salon|barbershop|"
    r"funeral|grocery|hardware|insurance\s+agenc|mortgage\s+company|"
    r"prepaid\s+legal\s+services|pre.paid\s+legal|uaw.?gm\b)\b",
    re.I,
)

COUNTY_CONFIGS = {
    "oklahoma-county-ok": {
        "county": "Oklahoma", "state": "OK", "state_name": "oklahoma", "msa": "Oklahoma City",
        "cities": ["Oklahoma City", "Edmond", "Moore", "Midwest City", "Del City",
                   "Bethany", "Warr Acres", "Nichols Hills", "The Village", "Choctaw"],
    },
    "tulsa-county-ok": {
        "county": "Tulsa", "state": "OK", "state_name": "oklahoma", "msa": "Tulsa",
        "cities": ["Tulsa", "Broken Arrow", "Owasso", "Sand Springs", "Jenks",
                   "Bixby", "Collinsville", "Glenpool", "Catoosa"],
    },
    "cleveland-county-ok": {
        "county": "Cleveland", "state": "OK", "state_name": "oklahoma", "msa": "Oklahoma City",
        "cities": ["Norman", "Moore", "Noble", "Lexington"],
    },
    "canadian-county-ok": {
        "county": "Canadian", "state": "OK", "state_name": "oklahoma", "msa": "Oklahoma City",
        "cities": ["Yukon", "Mustang", "El Reno", "Piedmont", "Tuttle", "Weatherford"],
    },
    "rogers-county-ok": {
        "county": "Rogers", "state": "OK", "state_name": "oklahoma", "msa": "Tulsa",
        "cities": ["Claremore", "Inola", "Chelsea", "Oologah"],
    },
    "wagoner-county-ok": {
        "county": "Wagoner", "state": "OK", "state_name": "oklahoma", "msa": "Tulsa",
        "cities": ["Wagoner", "Coweta", "Porter"],
    },
    "grady-county-ok": {
        "county": "Grady", "state": "OK", "state_name": "oklahoma", "msa": "Oklahoma City",
        "cities": ["Chickasha", "Blanchard", "Ninnekah", "Rush Springs", "Minco"],
    },
    "pottawatomie-county-ok": {
        "county": "Pottawatomie", "state": "OK", "state_name": "oklahoma", "msa": "Oklahoma City",
        "cities": ["Shawnee", "Tecumseh", "McLoud", "Meeker", "Prague"],
    },
    "creek-county-ok": {
        "county": "Creek", "state": "OK", "state_name": "oklahoma", "msa": "Tulsa",
        "cities": ["Sapulpa", "Bristow", "Drumright", "Mannford"],
    },
    "mcclain-county-ok": {
        "county": "McClain", "state": "OK", "state_name": "oklahoma", "msa": "Oklahoma City",
        "cities": ["Purcell", "Newcastle", "Lindsay", "Washington"],
    },
    "logan-county-ok": {
        "county": "Logan", "state": "OK", "state_name": "oklahoma", "msa": "Oklahoma City",
        "cities": ["Guthrie", "Crescent", "Cashion"],
    },
    "washington-county-ok": {
        "county": "Washington", "state": "OK", "state_name": "oklahoma", "msa": "Tulsa",
        "cities": ["Bartlesville", "Dewey", "Copan"],
    },
    "okmulgee-county-ok": {
        "county": "Okmulgee", "state": "OK", "state_name": "oklahoma", "msa": "Tulsa",
        "cities": ["Okmulgee", "Henryetta", "Beggs", "Morris"],
    },
    "osage-county-ok": {
        "county": "Osage", "state": "OK", "state_name": "oklahoma", "msa": "Tulsa",
        "cities": ["Pawhuska", "Hominy", "Fairfax", "Barnsdall"],
    },
}

PRIORITY_SCORES = {
    "Criminal Defense": 5, "DUI": 5, "Personal Injury": 5,
    "Medical Malpractice": 5, "Workers Compensation": 5, "Family Law": 4,
    "General Practice": 4, "Civil Litigation": 3, "Employment Law": 3,
    "Estate Planning": 2, "Bankruptcy": 2, "Real Estate": 2,
    "Business Law": 2, "Immigration": 2, "Tax Law": 2,
    "Social Security Disability": 2, "General": 1,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.lower())


def _norm_domain(url: str) -> str:
    if not url:
        return ""
    try:
        return urlparse(url).netloc.lower().lstrip("www.")
    except Exception:
        return ""


def _is_dir_url(url: str) -> bool:
    if not url:
        return True
    domain = _norm_domain(url)
    for d in _DIRECTORY_DOMAINS:
        if domain == d or domain.endswith("." + d):
            return True
    return False


def _ok_phone(phone: str) -> bool:
    """True if phone is an OK area code or toll-free."""
    return bool(_OK_AREA.match(phone) or _TOLLFREE.match(phone))


def _fetch(url: str, delay: float = 2.0, timeout: int = 12) -> requests.Response | None:
    for verify in (True, False):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout, verify=verify, allow_redirects=True)
            time.sleep(delay)
            if r.status_code == 200:
                return r
            return None
        except Exception:
            if verify:
                time.sleep(delay)
                continue
            time.sleep(delay)
    return None


def _load_existing(slug: str) -> tuple[set, set, set]:
    path = DATA_DIR / f"{slug}.csv"
    rows = list(csv.DictReader(path.open()))
    names = {_norm(r["law_firm_name"]) for r in rows}
    domains = {_norm_domain(r["website"]) for r in rows if r.get("website")}
    phones = {re.sub(r"[^\d]", "", r["phone_number"]) for r in rows if r.get("phone_number")}
    return names, domains, phones


def _make_row(name: str, city: str, cfg: dict, website: str = "",
              phone: str = "", practice: str = "General", address: str = "",
              zip_code: str = "") -> dict:
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
        "street_address": address,
        "zip_code": zip_code,
        "msa": cfg.get("msa", ""),
        "priority": priority,
        "number_of_lawyers": "",
    }


# ---------------------------------------------------------------------------
# Justia: directory listing + profile scraping
# ---------------------------------------------------------------------------

def _justia_dir_page(city: str, state_name: str, page: int = 1) -> tuple[list[dict], int]:
    """Get attorneys from a Justia directory listing page (JSON-LD)."""
    slug = city.lower().replace(" ", "-").replace("'", "")
    url = f"https://www.justia.com/lawyers/{state_name}/{slug}"
    if page > 1:
        url += f"?page={page}"
    r = _fetch(url, delay=2.5)
    if not r:
        return [], 0
    soup = BeautifulSoup(r.text, "html.parser")

    attorneys = []
    for script in soup.find_all("script"):
        text = script.string or ""
        if '"@type":"Person"' not in text:
            continue
        m = re.search(r"(\[.*?\"@type\":\"Person\".*?\])", text, re.DOTALL)
        if m:
            try:
                attorneys = json.loads(m.group(1))
            except Exception:
                pass
        break

    pages = [
        int(re.search(r"page=(\d+)", a["href"]).group(1))
        for a in soup.find_all("a", href=re.compile(r"page=\d+"))
        if re.search(r"page=(\d+)", a["href"])
    ]
    max_page = max(pages) if pages else 1
    return attorneys, max_page


def _justia_profile(profile_url: str) -> dict:
    """
    Fetch a Justia attorney profile and extract firm name, website, phone, city, zip.
    Returns dict with keys: firm_name, website, phone, city, zip_code, address, practice.
    """
    r = _fetch(profile_url, delay=1.5)
    if not r:
        return {}
    soup = BeautifulSoup(r.text, "html.parser")
    html = r.text

    result = {}

    # --- Firm name from JSON in raw HTML ---
    # Pattern: "name":"<FirmName>","description":[...],"address":{"@type":"PostalAddress"
    m = re.search(
        r'"name"\s*:\s*"([^"]{3,80})"\s*,\s*"description".*?"address"\s*:\s*\{.*?"PostalAddress"',
        html, re.DOTALL
    )
    if m:
        result["firm_name"] = m.group(1).strip()

    # --- Firm name fallback: LocalBusiness in JSON-LD ---
    if not result.get("firm_name"):
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                if isinstance(data, dict) and data.get("@type") in ("LocalBusiness", "Organization", "LawFirm"):
                    result["firm_name"] = data.get("name", "")
                    if not result.get("city"):
                        addr = data.get("address", {})
                        result["city"] = addr.get("addressLocality", "")
                        result["zip_code"] = addr.get("postalCode", "")
                        result["address"] = " ".join(
                            addr.get("streetAddress", []) if isinstance(addr.get("streetAddress"), list)
                            else [addr.get("streetAddress", "")]
                        )
            except Exception:
                pass

    # --- Address / city from JSON ---
    addr_m = re.search(
        r'"addressLocality"\s*:\s*"([^"]+)".*?"postalCode"\s*:\s*"(\d{5})"',
        html, re.DOTALL
    )
    if addr_m and not result.get("city"):
        result["city"] = addr_m.group(1)
        result["zip_code"] = addr_m.group(2)

    # --- Website: look for "ViewWebsite" or "Website" external links ---
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True)
        if (
            href.startswith("http")
            and not _is_dir_url(href)
            and text.lower() in ("viewwebsite", "website", "visit website", "firm website")
        ):
            result["website"] = href
            break

    # --- Phone from HTML ---
    ph_m = _PHONE_RE.search(html)
    if ph_m:
        result["phone"] = ph_m.group()

    # --- Practice area ---
    pa_m = re.search(
        r"Personal Injury|Criminal Defense|Family Law|DUI|Workers Comp|"
        r"Bankruptcy|Estate Planning|Real Estate|Business Law|Employment|"
        r"Civil Litigation|Medical Malpractice|Immigration|Tax Law",
        html, re.I
    )
    result["practice"] = pa_m.group() if pa_m else "General"

    return result


def scrape_justia_county(slug: str, cfg: dict, existing_names: set, existing_phones: set) -> list[dict]:
    """Return list of new firm rows found via Justia for this county."""
    new_rows = []
    seen_phones = set(existing_phones)
    seen_names = set(existing_names)

    for city in cfg["cities"]:
        print(f"    Justia [{city}]:", end="", flush=True)
        # Collect attorney entries across all pages
        attorney_entries = []
        _, max_page = _justia_dir_page(city, cfg["state_name"], 1)
        # Get up to 10 pages
        for page in range(1, min(max_page + 1, 11)):
            entries, _ = _justia_dir_page(city, cfg["state_name"], page)
            attorney_entries.extend(entries)
            if page < max_page:
                time.sleep(1.0)

        # Filter: only visit profiles for unknown phones
        new_count = 0
        for entry in attorney_entries:
            phone_raw = entry.get("telephone", "")
            phone_digits = re.sub(r"[^\d]", "", phone_raw)
            profile_url = entry.get("url", "")

            # Skip if phone already in data
            if phone_digits and phone_digits in seen_phones:
                continue
            if not profile_url:
                continue

            # Visit profile to get firm info
            prof = _justia_profile(profile_url)
            if not prof:
                continue

            firm_name = prof.get("firm_name", "").strip()
            if not firm_name:
                # Fall back to attorney's name from directory
                firm_name = entry.get("name", "").strip()
            if not firm_name or len(firm_name) < 3:
                continue
            if _ARTIFACT_RE.match(firm_name) or _NON_LAW_RE.search(firm_name):
                continue

            # Use city from profile if available, else from city we searched
            firm_city = prof.get("city", "").strip() or city
            website = prof.get("website", "")
            phone = prof.get("phone", "") or phone_raw
            address = prof.get("address", "")
            zip_code = prof.get("zip_code", "")
            practice = prof.get("practice", "General")

            # Deduplicate
            name_key = _norm(firm_name)
            if name_key in seen_names:
                # Update website if we found one and the existing row has none
                continue
            phone_key = re.sub(r"[^\d]", "", phone)
            if phone_key and phone_key in seen_phones:
                continue

            # Validate phone area code
            if phone and not _ok_phone(phone):
                continue

            seen_names.add(name_key)
            if phone_key:
                seen_phones.add(phone_key)

            row = _make_row(firm_name, firm_city, cfg, website=website, phone=phone,
                            practice=practice, address=address, zip_code=zip_code)
            new_rows.append(row)
            new_count += 1

        print(f" +{new_count}")

    return new_rows


# ---------------------------------------------------------------------------
# SuperLawyers
# ---------------------------------------------------------------------------

def scrape_superlawyers_county(slug: str, cfg: dict, existing_names: set, existing_phones: set) -> list[dict]:
    new_rows = []
    seen_names = set(existing_names)
    seen_phones = set(existing_phones)

    for city in cfg["cities"]:
        city_slug = city.lower().replace(" ", "-")
        url = f"https://www.superlawyers.com/{cfg['state_name']}/city/{city_slug}/"
        r = _fetch(url, delay=2.0)
        if not r:
            continue

        soup = BeautifulSoup(r.text, "html.parser")

        # Extract from JSON-LD if present
        for script in soup.find_all("script"):
            text = script.string or ""
            if '"@type":"Person"' in text or '"@type": "Person"' in text:
                m = re.search(r"(\[.*?\"@type\".*?\"Person\".*?\])", text, re.DOTALL)
                if not m:
                    m = re.search(r"(\{.*?\"@type\".*?\"Person\".*?\})", text, re.DOTALL)
                if m:
                    try:
                        data = json.loads(m.group(1))
                        items = data if isinstance(data, list) else [data]
                        for item in items:
                            name = item.get("name", "")
                            phone = item.get("telephone", "")
                            if not name:
                                continue
                            name_key = _norm(name)
                            phone_key = re.sub(r"[^\d]", "", phone)
                            if name_key in seen_names:
                                continue
                            if phone_key and phone_key in seen_phones:
                                continue
                            if phone and not _ok_phone(phone):
                                continue
                            seen_names.add(name_key)
                            if phone_key:
                                seen_phones.add(phone_key)
                            new_rows.append(_make_row(name, city, cfg, phone=phone))
                    except Exception:
                        pass

        # HTML fallback
        for card in soup.find_all(["div", "li"], class_=re.compile(r"lawyer|attorney|profile|result", re.I)):
            name_tag = card.find(["h2", "h3", "h4", "a"])
            if not name_tag:
                continue
            name = name_tag.get_text(strip=True)
            if not name or len(name) < 4 or _ARTIFACT_RE.match(name):
                continue
            phone_m = _PHONE_RE.search(card.get_text())
            phone = phone_m.group() if phone_m else ""
            website = next(
                (a["href"] for a in card.find_all("a", href=True)
                 if a["href"].startswith("http") and not _is_dir_url(a["href"])),
                ""
            )
            name_key = _norm(name)
            phone_key = re.sub(r"[^\d]", "", phone)
            if name_key in seen_names:
                continue
            if phone_key and phone_key in seen_phones:
                continue
            if phone and not _ok_phone(phone):
                continue
            seen_names.add(name_key)
            if phone_key:
                seen_phones.add(phone_key)
            new_rows.append(_make_row(name, city, cfg, website=website, phone=phone))

    return new_rows


# ---------------------------------------------------------------------------
# FindLaw
# ---------------------------------------------------------------------------

def scrape_findlaw_county(slug: str, cfg: dict, existing_names: set, existing_phones: set) -> list[dict]:
    new_rows = []
    seen_names = set(existing_names)
    seen_phones = set(existing_phones)

    for city in cfg["cities"]:
        city_slug = city.lower().replace(" ", "+")
        url = f"https://lawyers.findlaw.com/lawyer/firm/practice/oklahoma/{city_slug}"
        r = _fetch(url, delay=1.5)
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")

        # FindLaw embeds results in JSON
        for script in soup.find_all("script"):
            text = script.string or ""
            if "name" in text and ("phone" in text or "telephone" in text) and "attorney" in text.lower():
                try:
                    m = re.search(r'"results"\s*:\s*(\[.*?\])', text, re.DOTALL)
                    if m:
                        results = json.loads(m.group(1))
                        for item in results:
                            name = item.get("name", item.get("firmName", "")).strip()
                            phone = item.get("phone", item.get("telephone", "")).strip()
                            website = item.get("website", item.get("url", "")).strip()
                            city_val = item.get("city", city).strip()
                            if not name or len(name) < 3:
                                continue
                            if _is_dir_url(website):
                                website = ""
                            name_key = _norm(name)
                            phone_key = re.sub(r"[^\d]", "", phone)
                            if name_key in seen_names:
                                continue
                            if phone_key and phone_key in seen_phones:
                                continue
                            if phone and not _ok_phone(phone):
                                continue
                            seen_names.add(name_key)
                            if phone_key:
                                seen_phones.add(phone_key)
                            new_rows.append(_make_row(name, city_val, cfg, website=website, phone=phone))
                except Exception:
                    pass

        # HTML fallback
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/profile/" not in href and "/firm/" not in href:
                continue
            name = a.get_text(strip=True)
            if not name or len(name) < 4 or _ARTIFACT_RE.match(name):
                continue
            name_key = _norm(name)
            if name_key in seen_names:
                continue
            seen_names.add(name_key)
            new_rows.append(_make_row(name, city, cfg))

    return new_rows


# ---------------------------------------------------------------------------
# Avvo
# ---------------------------------------------------------------------------

def scrape_avvo_county(slug: str, cfg: dict, existing_names: set, existing_phones: set) -> list[dict]:
    new_rows = []
    seen_names = set(existing_names)
    seen_phones = set(existing_phones)

    for city in cfg["cities"]:
        city_slug = city.lower().replace(" ", "-").replace("'", "")
        url = f"https://www.avvo.com/all-lawyers/{city_slug}-ok.html"
        r = _fetch(url, delay=2.0)
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")

        for card in soup.find_all(["div", "li"], class_=re.compile(r"lawyer|attorney|result|listing", re.I)):
            name_tag = card.find(["h2", "h3", "h4", "a"])
            if not name_tag:
                continue
            name = name_tag.get_text(strip=True)
            if not name or len(name) < 4 or _ARTIFACT_RE.match(name):
                continue
            phone_m = _PHONE_RE.search(card.get_text())
            phone = phone_m.group() if phone_m else ""
            website = next(
                (a["href"] for a in card.find_all("a", href=True)
                 if a["href"].startswith("http") and not _is_dir_url(a["href"])),
                ""
            )
            name_key = _norm(name)
            phone_key = re.sub(r"[^\d]", "", phone)
            if name_key in seen_names:
                continue
            if phone_key and phone_key in seen_phones:
                continue
            if phone and not _ok_phone(phone):
                continue
            seen_names.add(name_key)
            if phone_key:
                seen_phones.add(phone_key)
            new_rows.append(_make_row(name, city, cfg, website=website, phone=phone))

    return new_rows


# ---------------------------------------------------------------------------
# Main per-county driver
# ---------------------------------------------------------------------------

def discover_county(slug: str) -> int:
    cfg = COUNTY_CONFIGS.get(slug)
    if not cfg:
        print(f"  SKIP: no config for {slug}")
        return 0

    existing_names, existing_domains, existing_phones = _load_existing(slug)
    p = DATA_DIR / f"{slug}.csv"
    rows = list(csv.DictReader(p.open()))
    fieldnames = list(rows[0].keys())

    all_new: list[dict] = []

    # --- Justia (profile-level: firm name + website) ---
    print(f"  [{slug}] Justia:")
    j_rows = scrape_justia_county(slug, cfg, existing_names, existing_phones)
    all_new.extend(j_rows)
    # Update seen sets so other sources don't re-add same firms
    for r in j_rows:
        existing_names.add(_norm(r["law_firm_name"]))
        existing_phones.add(re.sub(r"[^\d]", "", r.get("phone_number", "")))

    # --- SuperLawyers ---
    print(f"  [{slug}] SuperLawyers...", end="", flush=True)
    sl_rows = scrape_superlawyers_county(slug, cfg, existing_names, existing_phones)
    all_new.extend(sl_rows)
    for r in sl_rows:
        existing_names.add(_norm(r["law_firm_name"]))
        existing_phones.add(re.sub(r"[^\d]", "", r.get("phone_number", "")))
    print(f" +{len(sl_rows)}")

    # --- FindLaw ---
    print(f"  [{slug}] FindLaw...", end="", flush=True)
    fl_rows = scrape_findlaw_county(slug, cfg, existing_names, existing_phones)
    all_new.extend(fl_rows)
    for r in fl_rows:
        existing_names.add(_norm(r["law_firm_name"]))
        existing_phones.add(re.sub(r"[^\d]", "", r.get("phone_number", "")))
    print(f" +{len(fl_rows)}")

    # --- Avvo ---
    print(f"  [{slug}] Avvo...", end="", flush=True)
    av_rows = scrape_avvo_county(slug, cfg, existing_names, existing_phones)
    all_new.extend(av_rows)
    print(f" +{len(av_rows)}")

    if all_new:
        combined = rows + all_new
        combined.sort(key=lambda r: (r.get("city", ""), r.get("law_firm_name", "")))
        with p.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(combined)
        print(f"  [{slug}] +{len(all_new)} new firms → {len(combined)} total")
    else:
        print(f"  [{slug}] 0 new firms")

    return len(all_new)


if __name__ == "__main__":
    slugs = sys.argv[1:] if sys.argv[1:] else list(COUNTY_CONFIGS.keys())
    print(f"Deep discovery for {len(slugs)} OK counties...\n")
    total = 0
    for slug in slugs:
        total += discover_county(slug)
        print()
    print(f"Total new firms found: {total}")
