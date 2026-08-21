"""
Deep practice area re-scraping for 'General' entries.

Improvements over the existing scraper:
1. Scans meta description + OG tags for practice area keywords
2. Detects and fetches practice-area sub-pages (/practice-areas/, /services/, etc.)
3. Checks schema.org ld+json for service types
4. Expanded keyword list with more triggers
5. SSL errors handled with verify=False fallback
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

CITY_CSV = Path("app/county-data/st-louis-city-mo.csv")
COUNTY_CSV = Path("app/county-data/st-louis-county-mo.csv")
MANIFEST = Path("app/county-data/manifest.json")

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
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ---------------------------------------------------------------------------
# Expanded practice area keywords
# ---------------------------------------------------------------------------

PRACTICE_KEYWORDS = {
    "Personal Injury": [
        "personal injury", "car accident", "auto accident", "vehicle accident",
        "slip and fall", "premises liability", "wrongful death", "injury attorney",
        "injury lawyer", "accident attorney", "accident lawyer", "catastrophic injury",
        "brain injury", "spinal cord", "burn injury", "products liability",
        "product liability", "dog bite", "motorcycle accident", "truck accident",
        "bicycle accident", "pedestrian accident", "uber accident", "rideshare accident",
        "construction accident", "fall injury", "negligence claim",
    ],
    "Family Law": [
        "family law", "divorce", "child custody", "child support", "spousal support",
        "alimony", "adoption", "prenuptial", "postnuptial", "family attorney",
        "family lawyer", "dissolution of marriage", "separation agreement",
        "paternity", "guardianship", "parenting plan", "visitation rights",
        "domestic relations", "family court", "marital property",
    ],
    "Criminal Defense": [
        "criminal defense", "criminal law", "felony", "misdemeanor", "criminal charges",
        "dui defense", "dwi defense", "criminal attorney", "criminal lawyer",
        "drug charges", "drug offense", "assault", "theft", "robbery", "burglary",
        "white collar crime", "fraud charges", "federal criminal", "probation violation",
        "sex crime", "expungement", "criminal record",
    ],
    "DUI": [
        "dui", "dwi", "drunk driving", "driving under the influence",
        "driving while intoxicated", "impaired driving", "alcohol-related driving",
        "breathalyzer", "field sobriety",
    ],
    "Estate Planning": [
        "estate planning", "wills and trusts", "living trust", "probate",
        "estate attorney", "estate lawyer", "power of attorney", "elder law",
        "will preparation", "trust administration", "estate administration",
        "advance directive", "healthcare directive", "living will", "succession",
        "inheritance", "beneficiary", "irrevocable trust", "revocable trust",
        "special needs trust", "asset protection", "wealth transfer",
    ],
    "Workers' Compensation": [
        "workers compensation", "workers' compensation", "work injury",
        "workplace injury", "workers comp", "on-the-job injury", "occupational injury",
        "work accident", "job injury", "injured at work", "work-related injury",
    ],
    "Bankruptcy": [
        "bankruptcy", "chapter 7", "chapter 13", "chapter 11", "debt relief",
        "debt attorney", "debt lawyer", "foreclosure defense", "creditor harassment",
        "wage garnishment", "debt discharge", "fresh start", "financial fresh start",
        "insolvency",
    ],
    "Business Law": [
        "business law", "corporate law", "business attorney", "business lawyer",
        "contracts", "llc formation", "business formation", "business litigation",
        "commercial law", "commercial litigation", "mergers and acquisitions",
        "partnership", "shareholder", "operating agreement", "franchise law",
        "business transactions", "corporate counsel", "general counsel",
        "non-disclosure", "nda", "trade secrets",
    ],
    "Real Estate": [
        "real estate", "property law", "real estate attorney", "real estate lawyer",
        "title insurance", "property attorney", "real estate transactions",
        "real estate closing", "property closing", "landlord", "tenant",
        "leases", "zoning", "land use", "easements", "eminent domain",
        "boundary disputes", "title disputes", "commercial real estate",
    ],
    "Immigration": [
        "immigration", "visa", "green card", "citizenship", "naturalization",
        "deportation", "removal", "immigration attorney", "immigration lawyer",
        "work visa", "h-1b", "asylum", "refugee", "daca", "uscis",
        "permanent resident", "adjustment of status",
    ],
    "Employment Law": [
        "employment law", "wrongful termination", "discrimination", "workplace harassment",
        "sexual harassment", "eeoc", "employment attorney", "employment lawyer",
        "hostile work environment", "retaliation", "whistleblower", "fmla",
        "age discrimination", "race discrimination", "disability discrimination",
        "wage theft", "overtime", "unpaid wages", "non-compete",
    ],
    "Medical Malpractice": [
        "medical malpractice", "medical negligence", "hospital negligence",
        "doctor negligence", "nursing home", "surgical error", "misdiagnosis",
        "medication error", "birth injury", "cerebral palsy", "erb's palsy",
        "delayed diagnosis", "anesthesia error",
    ],
    "Social Security Disability": [
        "social security", "disability benefits", "ssdi", "ssi",
        "disability attorney", "disability lawyer", "social security disability",
        "disability claim", "denied disability",
    ],
    "Civil Litigation": [
        "civil litigation", "civil trial", "civil dispute", "civil attorney",
        "civil lawyer", "commercial dispute", "business dispute", "contract dispute",
        "civil rights", "section 1983",
    ],
    "Intellectual Property": [
        "intellectual property", "trademark", "patent", "copyright", "ip law",
        "ip attorney", "trade dress", "licensing",
    ],
    "Tax Law": [
        "tax law", "tax attorney", "irs", "tax litigation", "tax relief",
        "tax controversy", "tax audit", "back taxes", "tax debt", "tax planning",
    ],
    "Sexual Assault": [
        "sexual assault", "sexual abuse", "rape", "sex offense", "molestation",
        "human trafficking",
    ],
    "General Practice": [
        "general practice", "general law", "full service law", "diverse practice",
        "broad range of legal",
    ],
}

# Canonical priority from config
PRIORITY_SCORES = {
    "Criminal Defense": 5, "DUI": 5, "Personal Injury": 5,
    "Medical Malpractice": 5, "Workers' Compensation": 5, "Workers Compensation": 5,
    "Sexual Assault": 4, "Family Law": 4, "General Practice": 4,
    "Employment Law": 3, "Nursing Home": 3, "Civil Litigation": 3,
    "Insurance Defense": 3, "Divorce": 3,
    "Estate Planning": 2, "Probate": 2, "Bankruptcy": 2,
    "Real Estate": 2, "Real Estate Law": 2, "Business Law": 2,
    "Immigration": 2, "Tax Law": 2, "Social Security Disability": 2,
    "Intellectual Property": 1, "General": 4,
}


def _get_priority(area: str) -> int:
    return PRIORITY_SCORES.get(area, 1)


# Sub-pages to try for practice area content
PRACTICE_PATHS = [
    "/practice-areas", "/practice-areas/", "/practice_areas",
    "/services", "/services/", "/legal-services",
    "/areas-of-practice", "/areas-of-law",
    "/what-we-do", "/expertise",
    "/attorneys", "/lawyers",
    "/about", "/about-us", "/our-firm",
]


def _fetch(url: str, timeout: int = 6) -> requests.Response | None:
    for verify in (True, False):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout, verify=verify,
                             allow_redirects=True)
            if r.status_code == 200:
                return r
        except Exception:
            if verify:
                continue
    return None


def _extract_areas_from_text(text: str) -> list[str]:
    lower = text.lower()
    found = []
    for area, keywords in PRACTICE_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            found.append(area)
    return found


def _extract_schema_types(soup: BeautifulSoup) -> list[str]:
    areas = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            text = json.dumps(data).lower()
            areas.extend(_extract_areas_from_text(text))
        except Exception:
            pass
    return areas


def _find_practice_page_urls(soup: BeautifulSoup, base_url: str) -> list[str]:
    """Find links to practice area pages in navigation."""
    candidates = []
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        text = a.get_text(strip=True).lower()
        combined = href + " " + text
        if any(w in combined for w in [
            "practice", "service", "area", "expertise", "what-we-do",
            "legal service", "our work",
        ]):
            full_url = urljoin(base_url, a["href"])
            # Only same-domain URLs
            if urlparse(full_url).netloc == urlparse(base_url).netloc:
                if full_url not in candidates:
                    candidates.append(full_url)
    return candidates[:5]


def deep_scrape_practice_areas(url: str, name: str) -> list[str]:
    """Fetch homepage + up to 2 sub-pages, extract practice area keywords."""
    resp = _fetch(url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    all_text_parts = []

    # 1. Meta description + OG tags (often explicitly list practice areas)
    for attr, prop in [("name", "description"), ("property", "og:description"),
                       ("name", "keywords"), ("property", "og:title")]:
        tag = soup.find("meta", attrs={attr: prop})
        if tag and tag.get("content"):
            all_text_parts.append(tag["content"])

    # 2. Page title
    title = soup.find("title")
    if title:
        all_text_parts.append(title.get_text())

    # 3. Schema.org
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            all_text_parts.append(json.dumps(json.loads(script.string or "")))
        except Exception:
            pass

    # 4. Full page text
    all_text_parts.append(soup.get_text(separator=" ", strip=True))

    # 5. Find and fetch practice area sub-pages (up to 2)
    practice_page_urls = _find_practice_page_urls(soup, url)

    # Also try common paths directly
    base = url.rstrip("/")
    for path in PRACTICE_PATHS[:8]:
        candidate = base + path
        if candidate not in practice_page_urls:
            practice_page_urls.append(candidate)
        if len(practice_page_urls) >= 12:
            break

    pages_fetched = 0
    for sub_url in practice_page_urls:
        if pages_fetched >= 2:
            break
        if sub_url == url:
            continue
        sub_resp = _fetch(sub_url, timeout=5)
        if sub_resp:
            sub_soup = BeautifulSoup(sub_resp.text, "lxml")
            all_text_parts.append(sub_soup.get_text(separator=" ", strip=True))
            pages_fetched += 1
            time.sleep(0.3)

    combined = " ".join(all_text_parts)
    return _extract_areas_from_text(combined)


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


def process_file(rows: list, label: str) -> int:
    targets = [r for r in rows if r["practice_area"] == "General" and r["website"].strip()]
    print(f"\n{label}: processing {len(targets)} General entries with websites")

    updated = 0
    for i, row in enumerate(targets, 1):
        try:
            areas = deep_scrape_practice_areas(row["website"], row["law_firm_name"])
        except Exception:
            areas = []

        if areas:
            # Filter out low-signal "General Practice" if other areas found
            specific = [a for a in areas if a != "General Practice"]
            use_areas = specific if specific else areas

            best = max(use_areas, key=_get_priority)
            best_priority = _get_priority(best)

            if best != "General" and best_priority >= _get_priority(row["practice_area"]):
                old = row["practice_area"]
                row["practice_area"] = best
                row["priority"] = str(best_priority)
                updated += 1
                if updated <= 10 or updated % 20 == 0:
                    print(f"  Updated: {row['law_firm_name']} → {best} (was {old})")

        if i % 25 == 0:
            print(f"  Progress: {i}/{len(targets)}, updated {updated}")

        time.sleep(0.5)

    print(f"{label}: {updated}/{len(targets)} practice areas updated")
    return updated


def main():
    print("Loading CSVs...")
    city_rows = load_csv(CITY_CSV)
    county_rows = load_csv(COUNTY_CSV)

    city_updated = process_file(city_rows, "CITY")
    county_updated = process_file(county_rows, "COUNTY")

    print("\nSaving...")
    save_csv(CITY_CSV, city_rows)
    save_csv(COUNTY_CSV, county_rows)

    manifest = json.loads(MANIFEST.read_text())
    for entry in manifest.get("counties", []):
        if entry["slug"] == "st-louis-city-mo":
            entry["count"] = len(city_rows)
        elif entry["slug"] == "st-louis-county-mo":
            entry["count"] = len(county_rows)
    MANIFEST.write_text(json.dumps(manifest, indent=2) + "\n")

    # Stats
    city_gen = sum(1 for r in city_rows if r["practice_area"] == "General")
    county_gen = sum(1 for r in county_rows if r["practice_area"] == "General")
    print(f"\n=== Final Stats ===")
    print(f"  City:   {len(city_rows)} firms, {city_gen} General ({100*city_gen//len(city_rows)}%)")
    print(f"  County: {len(county_rows)} firms, {county_gen} General ({100*county_gen//len(county_rows)}%)")
    print(f"  Total updated: {city_updated + county_updated}")


if __name__ == "__main__":
    main()
