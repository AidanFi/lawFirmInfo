"""
General-purpose county data cleanup script.
Usage: python3 clean_county.py <slug>  [--skip-scrape]

Runs:
1. Remove non-law and no-contact entries
2. Fix ZIP codes from street_address or city fallback
3. Deep practice area re-scraping
4. Update manifest
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
MANIFEST = DATA_DIR / "manifest.json"

FIELDNAMES = [
    "law_firm_name", "website", "google_business_profile", "legal_directory_listing",
    "city", "state", "county", "phone_number", "email", "practice_area",
    "street_address", "zip_code", "msa", "priority",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# ---------------------------------------------------------------------------
# Non-law business filters (in addition to pipeline's built-in filters)
# ---------------------------------------------------------------------------

_NON_LEGAL_PATTERNS = re.compile(
    r'\b(?:'
    r'sprinkler|plumbing|roofing|hvac|heating|cooling|electric|electrician|'
    r'restaurant|cafe|coffee|bar(?!\s+(?:association|exam))|tavern|pub |brewery|winery|'
    r'salon|barber|spa|massage|tattoo|nail\s+(?:studio|salon)|'
    r'auto\s+(?:repair|body|glass|dealer)|car\s+(?:wash|rental|dealer)|'
    r'tire\s+(?:center|shop)|oil\s+change|mechanic|towing|'
    r'hotel|motel|inn(?!\s+(?:at|on|law))|resort|vacation|'
    r'dentist|dental|orthodontic|optometry|optometrist|veterinar|vet\s+(?:clinic|hospital)|'
    r'daycare|child\s+care|preschool|head\s+start|'
    r'grocery|supermarket|pharmacy|drug\s+store|'
    r'gym|fitness|yoga|pilates|crossfit|'
    r'church|temple|mosque|synagogue|cathedral|ministry|'
    r'school\s+(?:district|board)|elementary|middle\s+school|high\s+school|'
    r'police\s+(?:department|station)|fire\s+(?:department|station)|'
    r'city\s+(?:hall|of\b)|county\s+(?:clerk|court|jail|library)|'
    r'park(?:\s+(?:and|&)\s+(?:rec|ride)|way|land\b)|playground|'
    r'insurance\s+(?:agency|company)(?!\s+defense)|'
    r'accounting\s+(?:firm|service)|cpa\s+(?:firm|office)|'
    r'financial\s+(?:advisor|planner|services|group)|'
    r'real\s+estate\s+(?:agent|company|group|services|team)(?!\s+law)|'
    r'mortgage|lending|bank(?:ing)?\s+(?:group|services)|'
    r'it\s+(?:firm|services|company|solutions)|tech(?:nology)?\s+(?:firm|company|solutions)|'
    r'staffing|temp\s+agency|'
    r'donut|pizza|burger|taco|sushi|'
    r'thrift\s+store|pawn\s+shop|pallet|'
    r'political\s+(?:party|committee)'
    r')\b',
    re.IGNORECASE,
)

_NON_LEGAL_EXACT = {
    "american fire sprinklers, llc",
    "stop light at page and midland",
    "trafficstop",
    "ywca pagedale head start",
    "the it firm",
    "shanfeld's accounting firm",
    "bluebird park playground area",
    "donut palace",
    "tommy's tiki palace",
    "dr. lawrence hoffman dmd",
    "journey dance party",
    "eise park lending library",
    "underground passageway",
    "pershing park (parkview)",
    "harveys pallets",
    "belieber palace",
    "morgans pad",
    "st. vincent de paul thrift store",
    "business law",  # not a firm name
    "work (law firm)",  # not a real firm
    "law & justice",  # not a real firm name
    "uplands park mo",
}

_POSITIVE_INDICATORS = re.compile(
    r'\b(?:law|legal|attorney|lawyer|counsel|firm|llc|llp|pllc|p\.a\.|'
    r'p\.c\.|chartered|esq|esquire|advocates|litigat|juris)\b',
    re.IGNORECASE,
)


def _is_likely_law_firm(name: str) -> bool:
    if name.lower().strip() in _NON_LEGAL_EXACT:
        return False
    if _NON_LEGAL_PATTERNS.search(name):
        return False
    return True


# ---------------------------------------------------------------------------
# ZIP fixing
# ---------------------------------------------------------------------------

_ZIP_RE = re.compile(r'\b(\d{5})\b')


def _is_valid_mo_zip(z: str, state: str) -> bool:
    """Check if a ZIP looks valid for Missouri (or the county's state)."""
    if state == "MO":
        # Missouri ZIPs: 630xx-658xx
        return bool(re.match(r'^6[3-5]\d{3}$', z))
    elif state == "KS":
        # Kansas ZIPs: 660xx-679xx
        return bool(re.match(r'^6[6-7]\d{3}$', z))
    # Generic: any 5-digit ZIP is better than nothing
    return bool(re.match(r'^\d{5}$', z))


def _fix_zip(row: dict, city_zip_map: dict) -> str:
    state = row.get("state", "MO")
    current = row["zip_code"].strip()
    if current and _is_valid_mo_zip(current, state):
        return current
    # Try to extract from street_address
    street = row.get("street_address", "").strip()
    m = _ZIP_RE.search(street)
    if m and _is_valid_mo_zip(m.group(1), state):
        return m.group(1)
    # City fallback
    city = row["city"].strip()
    return city_zip_map.get(city, current)


# ---------------------------------------------------------------------------
# Practice area deep scraping
# ---------------------------------------------------------------------------

PRACTICE_KEYWORDS = {
    "Personal Injury": [
        "personal injury", "car accident", "auto accident", "vehicle accident",
        "slip and fall", "premises liability", "wrongful death", "injury attorney",
        "injury lawyer", "accident attorney", "accident lawyer", "catastrophic injury",
        "brain injury", "spinal cord", "products liability", "product liability",
        "dog bite", "motorcycle accident", "truck accident", "bicycle accident",
        "pedestrian accident", "construction accident", "negligence claim",
    ],
    "Family Law": [
        "family law", "divorce", "child custody", "child support", "spousal support",
        "alimony", "adoption", "prenuptial", "family attorney", "family lawyer",
        "dissolution of marriage", "paternity", "guardianship", "parenting plan",
        "visitation rights", "domestic relations", "family court",
    ],
    "Criminal Defense": [
        "criminal defense", "criminal law", "felony", "misdemeanor",
        "dui defense", "dwi defense", "criminal attorney", "criminal lawyer",
        "drug charges", "drug offense", "assault charges", "theft charges",
        "white collar crime", "fraud charges", "federal criminal", "probation violation",
        "sex crime", "expungement",
    ],
    "DUI": [
        "dui", "dwi", "drunk driving", "driving under the influence",
        "driving while intoxicated", "impaired driving",
    ],
    "Estate Planning": [
        "estate planning", "wills and trusts", "living trust", "probate",
        "estate attorney", "estate lawyer", "power of attorney", "elder law",
        "will preparation", "trust administration", "estate administration",
        "advance directive", "living will", "irrevocable trust", "revocable trust",
        "special needs trust", "asset protection", "inheritance", "beneficiary",
    ],
    "Workers' Compensation": [
        "workers compensation", "workers' compensation", "work injury",
        "workplace injury", "workers comp", "on-the-job injury",
        "work accident", "job injury", "injured at work",
    ],
    "Bankruptcy": [
        "bankruptcy", "chapter 7", "chapter 13", "chapter 11", "debt relief",
        "debt attorney", "foreclosure defense", "wage garnishment", "debt discharge",
        "insolvency",
    ],
    "Business Law": [
        "business law", "corporate law", "business attorney", "business lawyer",
        "llc formation", "business formation", "business litigation",
        "commercial law", "commercial litigation", "mergers and acquisitions",
        "shareholder", "operating agreement", "business transactions",
        "corporate counsel", "general counsel", "trade secrets",
    ],
    "Real Estate": [
        "real estate", "property law", "real estate attorney", "real estate lawyer",
        "real estate closing", "property closing", "landlord", "tenant rights",
        "zoning", "land use", "easements", "eminent domain",
        "boundary disputes", "commercial real estate",
    ],
    "Immigration": [
        "immigration", "visa", "green card", "citizenship", "naturalization",
        "deportation", "removal", "immigration attorney", "immigration lawyer",
        "work visa", "h-1b", "asylum", "daca", "uscis",
    ],
    "Employment Law": [
        "employment law", "wrongful termination", "discrimination",
        "workplace harassment", "sexual harassment", "eeoc",
        "employment attorney", "employment lawyer", "hostile work environment",
        "retaliation", "whistleblower", "fmla", "age discrimination",
        "wage theft", "overtime", "unpaid wages", "non-compete",
    ],
    "Medical Malpractice": [
        "medical malpractice", "medical negligence", "hospital negligence",
        "doctor negligence", "nursing home", "surgical error", "misdiagnosis",
        "medication error", "birth injury", "cerebral palsy",
    ],
    "Social Security Disability": [
        "social security", "disability benefits", "ssdi", "ssi",
        "disability attorney", "disability lawyer", "disability claim",
    ],
    "Civil Litigation": [
        "civil litigation", "civil trial", "civil dispute",
        "commercial dispute", "business dispute", "contract dispute", "civil rights",
    ],
    "Tax Law": [
        "tax law", "tax attorney", "irs", "tax litigation", "tax relief",
        "tax controversy", "tax audit", "back taxes", "tax debt",
    ],
    "Intellectual Property": [
        "intellectual property", "trademark", "patent", "copyright",
        "ip attorney", "licensing",
    ],
    "Sexual Assault": [
        "sexual assault", "sexual abuse", "sex offense", "molestation",
    ],
}

PRIORITY_SCORES = {
    "Criminal Defense": 5, "DUI": 5, "Personal Injury": 5,
    "Medical Malpractice": 5, "Workers' Compensation": 5,
    "Sexual Assault": 4, "Family Law": 4, "General Practice": 4,
    "Employment Law": 3, "Nursing Home": 3, "Civil Litigation": 3,
    "Insurance Defense": 3, "Divorce": 3,
    "Estate Planning": 2, "Probate": 2, "Bankruptcy": 2,
    "Real Estate": 2, "Real Estate Law": 2, "Business Law": 2,
    "Immigration": 2, "Tax Law": 2, "Social Security Disability": 2,
    "Intellectual Property": 1, "General": 4, "General Practice": 4,
}

PRACTICE_PATHS = [
    "/practice-areas", "/practice-areas/", "/services", "/services/",
    "/areas-of-practice", "/areas-of-law", "/legal-services",
    "/what-we-do", "/expertise", "/attorneys", "/about", "/about-us",
]


def _get_priority(area: str) -> int:
    return PRIORITY_SCORES.get(area, 1)


def _fetch(url: str, timeout: int = 6) -> requests.Response | None:
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


def _extract_areas(text: str) -> list[str]:
    lower = text.lower()
    return [area for area, kws in PRACTICE_KEYWORDS.items()
            if any(kw in lower for kw in kws)]


def deep_scrape(url: str) -> list[str]:
    resp = _fetch(url)
    if not resp:
        return []
    soup = BeautifulSoup(resp.text, "lxml")
    parts = []
    for attr, prop in [("name", "description"), ("property", "og:description"),
                       ("name", "keywords"), ("property", "og:title")]:
        tag = soup.find("meta", attrs={attr: prop})
        if tag and tag.get("content"):
            parts.append(tag["content"])
    title = soup.find("title")
    if title:
        parts.append(title.get_text())
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            parts.append(json.dumps(json.loads(script.string or "")))
        except Exception:
            pass
    parts.append(soup.get_text(separator=" ", strip=True))

    # Sub-pages
    sub_urls = []
    for a in soup.find_all("a", href=True):
        h = a["href"].lower()
        t = a.get_text(strip=True).lower()
        if any(w in h + " " + t for w in ["practice", "service", "area", "expertise"]):
            full = urljoin(url, a["href"])
            if urlparse(full).netloc == urlparse(url).netloc and full not in sub_urls:
                sub_urls.append(full)
    base = url.rstrip("/")
    for path in PRACTICE_PATHS:
        c = base + path
        if c not in sub_urls:
            sub_urls.append(c)

    fetched = 0
    for sub in sub_urls:
        if fetched >= 2 or sub == url:
            continue
        r = _fetch(sub, timeout=5)
        if r:
            parts.append(BeautifulSoup(r.text, "lxml").get_text(separator=" ", strip=True))
            fetched += 1
            time.sleep(0.2)

    return _extract_areas(" ".join(parts))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def load_csv(path: Path) -> list:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def save_csv(path: Path, rows: list):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 clean_county.py <slug> [--skip-scrape]")
        sys.exit(1)

    slug = sys.argv[1]
    skip_scrape = "--skip-scrape" in sys.argv
    csv_path = DATA_DIR / f"{slug}.csv"

    if not csv_path.exists():
        print(f"ERROR: {csv_path} not found")
        sys.exit(1)

    # Load city→ZIP mapping from county config
    from scraper.county.config import COUNTY_DEFINITIONS
    county_key = next(
        (k for k, v in COUNTY_DEFINITIONS.items() if v["slug"] == slug), None
    )
    city_zip_map = {}
    state = "MO"
    if county_key:
        config = COUNTY_DEFINITIONS[county_key]
        state = config["state"]
        zips = config.get("zip_codes", [])
        cities = config.get("cities", [])
        # Map each city to the first ZIP from the config (rough primary ZIP)
        if zips and cities:
            for i, city in enumerate(cities):
                if i < len(zips):
                    city_zip_map[city] = zips[i]

    print(f"Loading {csv_path}...")
    rows = load_csv(csv_path)
    print(f"  Loaded {len(rows)} rows")

    # ------------------------------------------------------------------
    # Step 1: Remove non-law businesses and no-contact entries
    # ------------------------------------------------------------------
    print("\nStep 1: Filtering non-law businesses only...")
    before = len(rows)
    rows = [r for r in rows if _is_likely_law_firm(r["law_firm_name"])]
    non_law_removed = before - len(rows)

    # NOTE: Per the include-all-firms policy, we do NOT remove entries for
    # missing contact data (no phone/website/email). Every legitimate law
    # firm/attorney is kept regardless of how reachable it is.
    no_contact_removed = 0

    print(f"  Non-law removed: {non_law_removed}")
    print(f"  No-contact removed: {no_contact_removed} (policy: keep all firms)")

    # ------------------------------------------------------------------
    # Step 2: Fix ZIP codes
    # ------------------------------------------------------------------
    print("\nStep 2: Fixing ZIP codes...")
    zip_fixed = 0
    for r in rows:
        new_zip = _fix_zip(r, city_zip_map)
        if new_zip != r["zip_code"]:
            r["zip_code"] = new_zip
            zip_fixed += 1
    print(f"  ZIPs fixed: {zip_fixed}")

    # ------------------------------------------------------------------
    # Step 3: Practice area deep scraping
    # ------------------------------------------------------------------
    if not skip_scrape:
        targets = [r for r in rows if r["practice_area"] == "General" and r["website"].strip()]
        print(f"\nStep 3: Deep scraping {len(targets)} General entries with websites...")
        updated = 0
        for i, row in enumerate(targets, 1):
            try:
                areas = deep_scrape(row["website"])
            except Exception:
                areas = []
            if areas:
                specific = [a for a in areas if a != "General Practice"]
                best = max(specific or areas, key=_get_priority)
                if best != "General":
                    row["practice_area"] = best
                    row["priority"] = str(_get_priority(best))
                    updated += 1
            if i % 25 == 0:
                print(f"  Progress: {i}/{len(targets)}, updated {updated}")
            time.sleep(0.5)
        print(f"  Practice areas updated: {updated}/{len(targets)}")
    else:
        print("\nStep 3: Skipping practice area scraping (--skip-scrape)")

    # ------------------------------------------------------------------
    # Save and update manifest
    # ------------------------------------------------------------------
    print(f"\nSaving {csv_path}...")
    save_csv(csv_path, rows)

    manifest = json.loads(MANIFEST.read_text())
    updated_manifest = False
    for entry in manifest.get("counties", []):
        if entry["slug"] == slug:
            entry["firm_count"] = len(rows)
            entry["last_updated"] = "2026-05-31"
            updated_manifest = True
            break
    if not updated_manifest:
        print("  WARNING: slug not found in manifest — run pipeline first to add it")
    else:
        MANIFEST.write_text(json.dumps(manifest, indent=2) + "\n")

    # ------------------------------------------------------------------
    # Quality report
    # ------------------------------------------------------------------
    n = len(rows)
    has_web = sum(1 for r in rows if r["website"].strip())
    has_phone = sum(1 for r in rows if r["phone_number"].strip())
    has_email = sum(1 for r in rows if r["email"].strip())
    general = sum(1 for r in rows if r["practice_area"] == "General")
    bad_zips = [r for r in rows if r["zip_code"] and not _is_valid_mo_zip(r["zip_code"], state)]

    print(f"\n=== {slug} Quality Report ===")
    print(f"  Total firms: {n}")
    print(f"  Website:  {has_web}/{n} ({100*has_web//n if n else 0}%)")
    print(f"  Phone:    {has_phone}/{n} ({100*has_phone//n if n else 0}%)")
    print(f"  Email:    {has_email}/{n} ({100*has_email//n if n else 0}%)")
    print(f"  General:  {general}/{n} ({100*general//n if n else 0}%)")
    print(f"  Bad ZIPs: {len(bad_zips)}")
    if bad_zips[:5]:
        for r in bad_zips[:5]:
            print(f"    {r['law_firm_name']} | {r['city']} | {r['zip_code']}")


if __name__ == "__main__":
    main()
