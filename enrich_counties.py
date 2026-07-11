#!/usr/bin/env python3
"""
Deep enrichment pass for county CSVs.
- Practice area scraping: up to 6 inner pages, prioritized by URL type
- Email harvesting: extracts emails from websites that lack them
- Runs on all three target counties

Usage: python3 enrich_counties.py [--county <slug>]
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
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# ---------------------------------------------------------------------------
# Practice area keywords
# ---------------------------------------------------------------------------

PRACTICE_KEYWORDS = {
    "Personal Injury": [
        "personal injury", "car accident", "auto accident", "vehicle accident",
        "slip and fall", "premises liability", "wrongful death", "injury attorney",
        "injury lawyer", "accident attorney", "accident lawyer", "catastrophic injury",
        "brain injury", "spinal cord", "products liability", "dog bite",
        "motorcycle accident", "truck accident", "bicycle accident",
        "pedestrian accident", "construction accident",
    ],
    "Family Law": [
        "family law", "divorce", "child custody", "child support", "spousal support",
        "alimony", "adoption", "prenuptial", "family attorney", "family lawyer",
        "dissolution of marriage", "paternity", "guardianship", "parenting plan",
        "visitation rights", "domestic relations",
    ],
    "Criminal Defense": [
        "criminal defense", "criminal law", "felony", "misdemeanor",
        "dui defense", "dwi defense", "criminal attorney", "criminal lawyer",
        "drug charges", "assault charges", "theft charges",
        "white collar crime", "fraud charges", "federal criminal",
        "probation violation", "sex crime", "expungement", "traffic ticket",
        "traffic violation",
    ],
    "DUI": [
        "dui", "dwi", "drunk driving", "driving under the influence",
        "driving while intoxicated", "impaired driving",
    ],
    "Estate Planning": [
        "estate planning", "wills and trusts", "living trust", "probate",
        "estate attorney", "estate lawyer", "power of attorney", "elder law",
        "will preparation", "trust administration", "estate administration",
        "advance directive", "living will", "irrevocable trust",
        "revocable trust", "asset protection",
    ],
    "Workers' Compensation": [
        "workers compensation", "workers' compensation", "work injury",
        "workplace injury", "workers comp", "on-the-job injury",
        "work accident", "job injury", "injured at work",
    ],
    "Bankruptcy": [
        "bankruptcy", "chapter 7", "chapter 13", "chapter 11", "debt relief",
        "debt attorney", "foreclosure defense", "wage garnishment",
        "debt discharge", "insolvency",
    ],
    "Business Law": [
        "business law", "corporate law", "business attorney", "business lawyer",
        "llc formation", "business formation", "business litigation",
        "commercial law", "commercial litigation", "mergers and acquisitions",
        "shareholder", "operating agreement", "corporate counsel",
        "general counsel", "trade secrets", "contracts",
    ],
    "Real Estate": [
        "real estate law", "property law", "real estate attorney",
        "real estate lawyer", "real estate closing", "landlord", "tenant rights",
        "zoning", "land use", "easements", "eminent domain",
        "boundary disputes", "commercial real estate",
    ],
    "Immigration": [
        "immigration", "visa", "green card", "citizenship", "naturalization",
        "deportation", "removal", "immigration attorney", "immigration lawyer",
        "work visa", "h-1b", "asylum", "daca",
    ],
    "Employment Law": [
        "employment law", "wrongful termination", "discrimination",
        "workplace harassment", "sexual harassment", "eeoc",
        "employment attorney", "employment lawyer",
        "hostile work environment", "retaliation", "whistleblower",
        "fmla", "wage theft", "overtime", "unpaid wages", "non-compete",
    ],
    "Medical Malpractice": [
        "medical malpractice", "medical negligence", "hospital negligence",
        "doctor negligence", "nursing home abuse", "surgical error",
        "misdiagnosis", "medication error", "birth injury",
    ],
    "Social Security Disability": [
        "social security disability", "ssdi", "ssi",
        "disability attorney", "disability lawyer", "disability claim",
        "disability benefits",
    ],
    "Civil Litigation": [
        "civil litigation", "civil trial", "civil dispute",
        "commercial dispute", "business dispute", "contract dispute",
        "civil rights",
    ],
    "Tax Law": [
        "tax law", "tax attorney", "irs", "tax litigation", "tax relief",
        "tax controversy", "tax audit",
    ],
    "Intellectual Property": [
        "intellectual property", "trademark", "patent", "copyright",
        "ip attorney", "licensing agreement",
    ],
    "Sexual Assault": [
        "sexual assault", "sexual abuse", "sex offense", "molestation",
    ],
    "General Practice": [
        "general practice", "full service law", "all areas of law",
    ],
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


def _get_priority(area: str) -> int:
    return PRIORITY_SCORES.get(area, 1)


def _extract_areas(text: str) -> list[str]:
    lower = text.lower()
    return [area for area, kws in PRACTICE_KEYWORDS.items()
            if any(kw in lower for kw in kws)]


# ---------------------------------------------------------------------------
# Email extraction
# ---------------------------------------------------------------------------

_EMAIL_RE = re.compile(
    r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,6}\b'
)

_SKIP_EMAIL_DOMAINS = {
    "example.com", "domain.com", "email.com", "yourwebsite.com",
    "youremail.com", "sentry.io", "wixpress.com", "squarespace.com",
    "wordpress.com", "godaddy.com", "google.com", "facebook.com",
    "twitter.com", "linkedin.com", "instagram.com", "yelp.com",
    "avvo.com", "martindale.com", "findlaw.com", "justia.com",
    "lawyerlegion.com", "superlawyers.com", "lawyers.com",
}

_SKIP_EMAIL_PREFIXES = {
    "noreply", "no-reply", "donotreply", "support", "info@wix",
    "mailer", "newsletter", "unsubscribe", "privacy",
}


def _is_valid_law_email(email: str) -> bool:
    email = email.lower()
    domain = email.split("@", 1)[-1]
    prefix = email.split("@", 1)[0]
    if domain in _SKIP_EMAIL_DOMAINS:
        return False
    if any(prefix.startswith(p) for p in _SKIP_EMAIL_PREFIXES):
        return False
    if len(email) > 80:
        return False
    return True


def _find_emails(soup: BeautifulSoup, raw_html: str) -> list[str]:
    emails = set()
    # mailto: links
    for tag in soup.find_all("a", href=re.compile(r'^mailto:', re.IGNORECASE)):
        m = _EMAIL_RE.search(tag["href"])
        if m:
            emails.add(m.group().lower())
    # Raw HTML scan (catches obfuscated/plaintext)
    for m in _EMAIL_RE.finditer(raw_html):
        emails.add(m.group().lower())
    return [e for e in sorted(emails) if _is_valid_law_email(e)]


# ---------------------------------------------------------------------------
# HTTP fetch
# ---------------------------------------------------------------------------

def _fetch(url: str, timeout: int = 5) -> requests.Response | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout,
                         verify=False, allow_redirects=True)
        if r.status_code == 200:
            return r
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Inner page discovery
# ---------------------------------------------------------------------------

# (priority, url_keywords) — lower = higher priority
_PAGE_PATTERNS = [
    (1, ["practice-area", "practice_area", "areas-of-practice", "areas-of-law",
         "legal-services", "what-we-do", "our-services", "expertise"]),
    (2, ["services", "practice"]),
    (3, ["attorney", "attorneys", "lawyers", "our-team", "our-lawyers",
         "our-people", "team", "staff", "professionals", "meet-the-team"]),
    (4, ["about", "about-us", "our-firm", "who-we-are", "our-story"]),
    (5, ["contact", "contact-us", "reach-us", "get-in-touch"]),
]

_HARDCODED_PATHS = [
    (1, "/practice-areas"), (1, "/areas-of-practice"),
    (1, "/areas-of-law"), (1, "/legal-services"), (1, "/services"),
    (2, "/attorneys"), (2, "/team"),
    (3, "/about"), (3, "/about-us"),
    (4, "/contact"),
]


def _rank_link(href: str, text: str) -> int | None:
    combined = (href + " " + text).lower()
    for priority, keywords in _PAGE_PATTERNS:
        if any(kw in combined for kw in keywords):
            return priority
    return None


def _collect_subpages(soup: BeautifulSoup, base_url: str) -> list[tuple[int, str]]:
    domain = urlparse(base_url).netloc
    seen = {base_url}
    pages = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True)
        priority = _rank_link(href, text)
        if priority is None:
            continue
        full = urljoin(base_url, href)
        if urlparse(full).netloc != domain:
            continue
        # Strip fragment
        full = full.split("#")[0]
        if full in seen:
            continue
        seen.add(full)
        pages.append((priority, full))

    # Add hardcoded paths for pages not discovered via nav
    base = base_url.rstrip("/")
    for priority, path in _HARDCODED_PATHS:
        c = base + path
        if c not in seen:
            seen.add(c)
            pages.append((priority, c))

    pages.sort(key=lambda x: x[0])
    return pages


# ---------------------------------------------------------------------------
# Deep scrape: practice areas + email
# ---------------------------------------------------------------------------

def deep_enrich(url: str, max_pages: int = 3) -> tuple[list[str], str | None]:
    """
    Returns (practice_areas, best_email).
    Fetches homepage + up to max_pages inner pages, prioritised by content type.
    """
    resp = _fetch(url)
    if not resp:
        return [], None

    soup = BeautifulSoup(resp.text, "lxml")

    # --- collect text from homepage ---
    parts = []
    for attr, prop in [
        ("name", "description"), ("property", "og:description"),
        ("name", "keywords"), ("property", "og:title"),
        ("name", "subject"),
    ]:
        tag = soup.find("meta", attrs={attr: prop})
        if tag and tag.get("content"):
            parts.append(tag["content"])

    title = soup.find("title")
    if title:
        parts.append(title.get_text())

    # h1/h2/h3 headings carry high-signal practice area terms
    for tag in soup.find_all(["h1", "h2", "h3"]):
        parts.append(tag.get_text(separator=" ", strip=True))

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            parts.append(json.dumps(data))
        except Exception:
            pass

    parts.append(soup.get_text(separator=" ", strip=True))

    emails = _find_emails(soup, resp.text)

    # --- inner pages ---
    subpages = _collect_subpages(soup, url)
    fetched = 0
    attempts = 0
    max_attempts = max_pages * 3  # try at most 3× the max pages before giving up
    for _, sub in subpages:
        if fetched >= max_pages or attempts >= max_attempts:
            break
        if sub == url:
            continue
        attempts += 1
        r = _fetch(sub, timeout=4)
        if not r:
            continue
        sub_soup = BeautifulSoup(r.text, "lxml")
        for tag in sub_soup.find_all(["h1", "h2", "h3"]):
            parts.append(tag.get_text(separator=" ", strip=True))
        parts.append(sub_soup.get_text(separator=" ", strip=True))
        emails += _find_emails(sub_soup, r.text)
        fetched += 1
        # Early exit: if we already have areas + email, stop
        if _extract_areas(" ".join(parts)) and emails:
            break
        time.sleep(0.15)

    areas = _extract_areas(" ".join(parts))

    # Deduplicate and pick best email
    unique_emails = list(dict.fromkeys(emails))
    best_email = unique_emails[0] if unique_emails else None

    return areas, best_email


# ---------------------------------------------------------------------------
# Per-county enrichment
# ---------------------------------------------------------------------------

def enrich_csv(slug: str) -> dict:
    path = DATA_DIR / f"{slug}.csv"
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    fieldnames = list(rows[0].keys()) if rows else []

    practice_targets = [r for r in rows
                        if r["practice_area"] == "General" and r["website"].strip()]
    email_targets = [r for r in rows
                     if not r["email"].strip() and r["website"].strip()]

    # Build a combined target set — websites we need to scrape
    # Some rows need both practice area and email; some need only one
    all_websites = {}  # website -> list of rows
    for r in practice_targets + email_targets:
        url = r["website"].strip()
        if url not in all_websites:
            all_websites[url] = []
        if r not in all_websites[url]:
            all_websites[url].append(r)

    # Deduplicate: multiple rows might share a website
    # (multi-office firms). We scrape once, apply to all.
    total = len(all_websites)
    print(f"\n  [{slug}] Scraping {total} unique websites "
          f"({len(practice_targets)} need practice area, "
          f"{len(email_targets)} need email)...")

    stats = {"practice_updated": 0, "email_updated": 0, "errors": 0}
    website_cache: dict[str, tuple[list[str], str | None]] = {}

    processed = 0
    for url, target_rows in all_websites.items():
        processed += 1
        if processed % 50 == 0:
            print(f"  Progress: {processed}/{total} | "
                  f"practice={stats['practice_updated']} "
                  f"email={stats['email_updated']}")
        try:
            areas, email = deep_enrich(url)
            website_cache[url] = (areas, email)
        except Exception:
            stats["errors"] += 1
            continue

        for row in target_rows:
            # Practice area update
            if row["practice_area"] == "General" and areas:
                specific = [a for a in areas if a not in ("General Practice", "General")]
                if specific:
                    best = max(specific, key=_get_priority)
                    row["practice_area"] = best
                    row["priority"] = str(_get_priority(best))
                    stats["practice_updated"] += 1
                elif "General Practice" in areas and row["practice_area"] == "General":
                    row["practice_area"] = "General Practice"
                    row["priority"] = str(_get_priority("General Practice"))
                    stats["practice_updated"] += 1

            # Email update
            if not row["email"].strip() and email:
                row["email"] = email
                stats["email_updated"] += 1

        time.sleep(0.2)

    # Save
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

    return stats


# ---------------------------------------------------------------------------
# Quality report
# ---------------------------------------------------------------------------

def quality_report(slug: str):
    with open(DATA_DIR / f"{slug}.csv", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    n = len(rows)
    has_web = sum(1 for r in rows if r["website"])
    has_phone = sum(1 for r in rows if r["phone_number"])
    has_email = sum(1 for r in rows if r["email"])
    general = sum(1 for r in rows if r["practice_area"] == "General")
    print(f"\n=== {slug} Quality Report ===")
    print(f"  Total: {n}")
    print(f"  Website:  {has_web}/{n} ({100*has_web//n}%)")
    print(f"  Phone:    {has_phone}/{n} ({100*has_phone//n}%)")
    print(f"  Email:    {has_email}/{n} ({100*has_email//n}%)")
    print(f"  General:  {general}/{n} ({100*general//n}%)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

TARGETS = [
    "jackson-county-mo", "greene-county-mo", "st-charles-county-mo",
    "johnson-county-ks", "wyandotte-county-ks", "leavenworth-county-ks",
    "miami-county-ks", "linn-county-ks",
]

if __name__ == "__main__":
    target_arg = None
    if "--county" in sys.argv:
        idx = sys.argv.index("--county")
        if idx + 1 < len(sys.argv):
            target_arg = sys.argv[idx + 1]

    slugs = [target_arg] if target_arg else TARGETS

    for slug in slugs:
        print(f"\n{'='*60}")
        print(f"  Enriching {slug}")
        print(f"{'='*60}")
        stats = enrich_csv(slug)
        print(f"\n  Results:")
        print(f"    Practice area updated: {stats['practice_updated']}")
        print(f"    Email updated:         {stats['email_updated']}")
        print(f"    Errors:                {stats['errors']}")
        quality_report(slug)
