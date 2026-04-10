#!/usr/bin/env python3
"""Enrich existing firms with website URLs, phone numbers, and emails.

Phase 1: Re-crawl FindLaw listing pages — extract phone + website from cards
Phase 2: Visit Avvo attorney profile pages — extract website
Phase 3: Visit firm websites — extract email + phone from actual sites

Usage:
    python -m scraper.enrich                    # Full run
    python -m scraper.enrich --skip-findlaw     # Skip FindLaw
    python -m scraper.enrich --skip-avvo        # Skip Avvo
    python -m scraper.enrich --skip-websites    # Skip firm website scraping
    python -m scraper.enrich --test             # Limit scope for testing
"""
import argparse
import json
import re
import shutil
import time
from datetime import datetime, timezone
from urllib.parse import quote_plus

from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests
import requests

from scraper.utils.normalize import normalize_firm_name, are_same_firm

_IMPERSONATE = "chrome"
_FL_BASE = "https://lawyers.findlaw.com"
INPUT_PATH = "app/firms_data.js"
BACKUP_PATH = "/tmp/firms_data_backup.js"

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; LawFirmDirectory/1.0)"}
SKIP_EMAIL_PATTERNS = re.compile(r"noreply|no-reply|admin@|webmaster@|example\.", re.I)

# Same practice areas as the original FindLaw scraper
_TOP_PRACTICE_AREAS = [
    ("Personal Injury", "personal-injury-plaintiff"),
    ("Family Law", "family-law"),
    ("Criminal Defense", "criminal-law"),
    ("Estate Planning", "estate-planning"),
    ("Business Law", "business-commercial-law"),
    ("Real Estate", "real-estate"),
    ("Bankruptcy", "bankruptcy-law"),
    ("Employment Law", "employment-labor-law"),
    ("Workers' Compensation", "workers-compensation"),
    ("DUI/DWI", "dui-dwi"),
    ("Immigration", "immigration"),
    ("Divorce", "divorce"),
    ("Medical Malpractice", "medical-malpractice"),
    ("Civil Litigation", "general-litigation"),
    ("Tax Law", "tax-law"),
    ("Intellectual Property", "intellectual-property"),
    ("Elder Law", "elder-law"),
    ("Insurance", "insurance-law"),
    ("Social Security Disability", "social-security-disability"),
    ("Probate", "probate-estate-administration"),
    ("Child Custody", "custody-visitation"),
    ("Wrongful Death", "wrongful-death"),
    ("Traffic Violations", "traffic-violations"),
    ("Trusts", "trusts"),
    ("Wills", "wills"),
    ("Construction", "construction-law"),
    ("Environmental Law", "environmental-law"),
    ("Civil Rights", "civil-rights"),
    ("Consumer Protection", "consumer-protection"),
    ("Government", "government"),
]


def _load_firms():
    with open(INPUT_PATH) as f:
        content = f.read()
    json_str = content[len("const FIRMS_DATA = "):-1]
    return json.loads(json_str)


def _save_firms(data):
    shutil.copy(INPUT_PATH, BACKUP_PATH)
    with open(INPUT_PATH, "w") as f:
        f.write("const FIRMS_DATA = ")
        json.dump(data, f, indent=2)
        f.write(";")
    print(f"[enrich] Saved to {INPUT_PATH}, backup at {BACKUP_PATH}")


def _cffi_get(url, delay=1.0):
    try:
        r = cffi_requests.get(url, impersonate=_IMPERSONATE, timeout=15)
        time.sleep(delay)
        if r.status_code == 200:
            return BeautifulSoup(r.text, "lxml")
    except Exception:
        pass
    return None


# ── Phase 1: FindLaw listing re-crawl ────────────────────────────────────────

def _get_city_slugs(pa_slug, delay):
    url = f"{_FL_BASE}/{pa_slug}/kansas/"
    soup = _cffi_get(url, delay=delay)
    if not soup:
        return []
    slugs = []
    prefix = f"/{pa_slug}/kansas/"
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if prefix in href and href.rstrip("/") != f"{_FL_BASE}{prefix}".rstrip("/"):
            m = re.search(rf"{re.escape(prefix)}([\w-]+)/?", href)
            if m and m.group(1) not in slugs:
                slugs.append(m.group(1))
    return slugs


def _extract_cards_with_contact(soup, city_slug):
    """Extract firm name, phone, and website from FindLaw listing cards."""
    results = []
    city = city_slug.replace("-", " ").title()

    for card in soup.find_all(class_="fl-serp-card"):
        title_el = card.find(class_="fl-serp-card-title")
        if not title_el:
            continue
        name = re.sub(r"Sponsored$", "", title_el.get_text(strip=True)).strip()
        if not name:
            continue

        phone = None
        tel = card.find("a", href=lambda h: h and h.startswith("tel:"))
        if tel:
            raw = re.sub(r"[^\d]", "", tel["href"].replace("tel:", ""))
            if len(raw) == 11 and raw.startswith("1"):
                raw = raw[1:]
            if len(raw) == 10:
                phone = f"({raw[:3]}) {raw[3:6]}-{raw[6:]}"

        website = None
        for a in card.find_all("a", href=True):
            text = a.get_text(strip=True).lower()
            href = a["href"]
            if "visit" in text and "website" in text and href.startswith("http"):
                # Strip tracking params
                website = re.sub(r"\?.*$", "", href)
                break

        if phone or website:
            results.append({"name": name, "city": city, "phone": phone, "website": website})

    return results


def _build_firm_index(firms):
    """Build a lookup index for fast firm matching."""
    by_city = {}
    for firm in firms:
        city = (firm.get("address") or {}).get("city", "").lower()
        if city:
            by_city.setdefault(city, []).append(firm)
    return by_city


def _match_and_enrich(firm_index, entry):
    """Try to match a FindLaw card entry to an existing firm and enrich it.
    Returns (websites_added, phones_added) as ints."""
    city = entry["city"].lower()
    candidates = firm_index.get(city, [])
    w = p = 0

    for firm in candidates:
        if are_same_firm(firm["name"], entry["name"]):
            if entry.get("website") and not firm.get("website"):
                firm["website"] = entry["website"]
                w = 1
            if entry.get("phone") and not firm.get("phone"):
                firm["phone"] = entry["phone"]
                p = 1
            return w, p
    return w, p


def enrich_from_findlaw(firms, delay=1.0, test_mode=False):
    """Re-crawl FindLaw listing pages, extract phone + website, match to firms."""
    pa_list = _TOP_PRACTICE_AREAS
    if test_mode:
        pa_list = pa_list[:3]

    firm_index = _build_firm_index(firms)

    print(f"[findlaw-enrich] Re-crawling {len(pa_list)} practice areas for contact data...")
    found_website = 0
    found_phone = 0
    pages = 0

    for pa_idx, (pa_name, pa_slug) in enumerate(pa_list):
        city_slugs = _get_city_slugs(pa_slug, delay=delay)
        if test_mode:
            city_slugs = city_slugs[:5]

        for city_slug in city_slugs:
            url = f"{_FL_BASE}/{pa_slug}/kansas/{city_slug}/"
            page = 1
            while url and page <= 10:
                soup = _cffi_get(url, delay=delay)
                if not soup:
                    break

                entries = _extract_cards_with_contact(soup, city_slug)
                for entry in entries:
                    w, p = _match_and_enrich(firm_index, entry)
                    found_website += w
                    found_phone += p

                pages += 1

                next_link = soup.find("a", string=re.compile(r"Next", re.I), href=True)
                if next_link:
                    href = next_link["href"]
                    if not href.startswith("http"):
                        href = f"{_FL_BASE}{href}"
                    url = href
                    page += 1
                else:
                    break

        if (pa_idx + 1) % 5 == 0:
            print(f"[findlaw-enrich] Progress: {pa_idx+1}/{len(pa_list)} areas, "
                  f"{pages} pages, +{found_website} websites, +{found_phone} phones")

    print(f"[findlaw-enrich] Done: {pages} pages, +{found_website} websites, +{found_phone} phones")
    return found_website, found_phone


# ── Phase 2: Avvo profile enrichment ─────────────────────────────────────────

def _extract_avvo_website(soup):
    """Extract the firm's website from an Avvo attorney profile page."""
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True).lower()
        if href.startswith("http") and "avvo.com" not in href:
            if any(kw in text for kw in ["website", "visit my website", "firm website", "view website"]):
                return href

    for section in soup.find_all(class_=lambda c: c and any(
            x in str(c).lower() for x in ["website", "contact", "sidebar"])):
        for a in section.find_all("a", href=True):
            href = a["href"]
            if (href.startswith("http") and "avvo.com" not in href
                    and not any(x in href for x in ["facebook.com", "twitter.com",
                                                      "linkedin.com", "google.com"])):
                if re.search(r"\.(com|law|legal|net|org|us|attorney|lawyer)(/|$)", href):
                    return href
    return None


def enrich_from_avvo(firms, delay=1.0, test_mode=False):
    """Visit Avvo profile pages for firms missing websites."""
    candidates = [f for f in firms if not f.get("website") and "avvo" in (f.get("sources") or [])]
    if test_mode:
        candidates = candidates[:100]

    print(f"[avvo-enrich] {len(candidates)} Avvo-sourced firms still missing websites...")

    # Re-crawl Avvo city listings to get profile URLs, then visit profiles
    # More efficient: crawl listings (20 per page) rather than search per firm
    from scraper.phases.avvo import _get_city_urls

    print("[avvo-enrich] Getting city index...")
    city_urls = _get_city_urls()
    if not city_urls:
        print("[avvo-enrich] Could not load city index")
        return 0

    # Build lookup of firms needing websites by city
    need_website = {}
    for firm in candidates:
        city = (firm.get("address") or {}).get("city", "").lower()
        if city:
            need_website.setdefault(city, []).append(firm)

    found = 0
    cities_checked = 0

    for city_name, city_url in city_urls:
        city_key = city_name.lower()
        if city_key not in need_website:
            continue

        # Crawl city pages, get profile URLs
        url = city_url
        page = 1
        while url and page <= 50:
            soup = _cffi_get(url, delay=delay)
            if not soup:
                break

            # Find all profile links with attorney names
            cards = soup.find_all(class_=lambda c: c and "organic-card" in
                                  (c if isinstance(c, str) else " ".join(c)))
            for card in cards:
                # Get attorney name
                name = None
                for a in card.find_all("a", href=True):
                    if "/attorneys/" in a["href"]:
                        text = a.get_text(strip=True)
                        if text and len(text) > 3 and " " in text and not re.search(r"\d", text):
                            name = text
                            profile_url = a["href"]
                            if not profile_url.startswith("http"):
                                profile_url = f"https://www.avvo.com{profile_url}"
                            break

                if not name:
                    continue

                # Check if this attorney matches any of our firms needing websites
                matched_firm = None
                for firm in need_website.get(city_key, []):
                    if firm.get("website"):
                        continue
                    if are_same_firm(firm["name"], name, threshold=85):
                        matched_firm = firm
                        break

                if not matched_firm:
                    continue

                # Visit profile to get website
                profile_soup = _cffi_get(profile_url, delay=delay)
                if not profile_soup:
                    continue

                website = _extract_avvo_website(profile_soup)
                if website:
                    matched_firm["website"] = website
                    found += 1

            # Pagination
            page += 1
            page_links = soup.find_all("a", href=lambda h: h and "page=" in str(h))
            max_page = 1
            for pl in page_links:
                try:
                    max_page = max(max_page, int(pl.get_text(strip=True)))
                except ValueError:
                    pass
            if page <= max_page:
                url = re.sub(r"\?page=\d+", "", city_url.rstrip("/"))
                url = f"{url}?page={page}"
            else:
                break

        cities_checked += 1
        if cities_checked % 20 == 0:
            print(f"[avvo-enrich] Progress: {cities_checked} cities, +{found} websites")

    print(f"[avvo-enrich] Done: {cities_checked} cities checked, +{found} websites")
    return found


# ── Phase 3: Website scraping for email/phone ────────────────────────────────

def _extract_email(soup):
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("mailto:"):
            addr = href[7:].split("?")[0].strip().lower()
            if addr and "@" in addr and not SKIP_EMAIL_PATTERNS.search(addr):
                return addr
    text = soup.get_text()
    emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
    for email in emails:
        if not SKIP_EMAIL_PATTERNS.search(email):
            return email.lower()
    return None


def _extract_phone(soup):
    for a in soup.find_all("a", href=True):
        if a["href"].startswith("tel:"):
            raw = re.sub(r"[^\d]", "", a["href"].replace("tel:", ""))
            if len(raw) == 11 and raw.startswith("1"):
                raw = raw[1:]
            if len(raw) == 10:
                return f"({raw[:3]}) {raw[3:6]}-{raw[6:]}"
    text = soup.get_text()
    match = re.search(r"\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}", text)
    if match:
        return match.group()
    return None


def enrich_from_websites(firms, delay=0.5, limit=None):
    """Visit firm websites to extract email and phone."""
    candidates = [f for f in firms if f.get("website") and (not f.get("email") or not f.get("phone"))]
    if limit:
        candidates = candidates[:limit]

    print(f"[website-enrich] Scraping {len(candidates)} firm websites for contact info...")
    found_email = 0
    found_phone = 0

    for i, firm in enumerate(candidates):
        url = firm["website"]
        try:
            resp = requests.get(url, timeout=8, headers=HEADERS, allow_redirects=True)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "lxml")

            if not firm.get("email"):
                email = _extract_email(soup)
                if email:
                    firm["email"] = email
                    found_email += 1
                else:
                    for path in ["/contact", "/contact-us"]:
                        try:
                            r2 = requests.get(url.rstrip("/") + path, timeout=5, headers=HEADERS)
                            if r2.status_code == 200:
                                contact_soup = BeautifulSoup(r2.text, "lxml")
                                email = _extract_email(contact_soup)
                                if email:
                                    firm["email"] = email
                                    found_email += 1
                                    break
                        except Exception:
                            pass

            if not firm.get("phone"):
                phone = _extract_phone(soup)
                if phone:
                    firm["phone"] = phone
                    found_phone += 1

        except Exception:
            pass

        time.sleep(delay)

        if (i + 1) % 100 == 0:
            print(f"[website-enrich] Progress: {i+1}/{len(candidates)}, "
                  f"emails: +{found_email}, phones: +{found_phone}")
            # Checkpoint save
            if (i + 1) % 500 == 0:
                data = _load_firms()
                data["firms"] = firms
                _save_firms(data)

    print(f"[website-enrich] Done: +{found_email} emails, +{found_phone} phones")
    return found_email, found_phone


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Enrich firm data with websites and contact info")
    parser.add_argument("--skip-findlaw", action="store_true")
    parser.add_argument("--skip-avvo", action="store_true")
    parser.add_argument("--skip-websites", action="store_true")
    parser.add_argument("--test", action="store_true", help="Limit scope for testing")
    parser.add_argument("--delay", type=float, default=1.0)
    args = parser.parse_args()

    data = _load_firms()
    firms = data["firms"]

    before_website = sum(1 for f in firms if f.get("website"))
    before_phone = sum(1 for f in firms if f.get("phone"))
    before_email = sum(1 for f in firms if f.get("email"))
    print(f"[enrich] Starting: {len(firms)} firms")
    print(f"[enrich] Before: {before_website} websites, {before_phone} phones, {before_email} emails")

    # Phase 1: FindLaw listing re-crawl
    if not args.skip_findlaw:
        enrich_from_findlaw(firms, delay=args.delay, test_mode=args.test)
        data["firms"] = firms
        _save_firms(data)

    # Phase 2: Avvo profile visits
    if not args.skip_avvo:
        enrich_from_avvo(firms, delay=args.delay, test_mode=args.test)
        data["firms"] = firms
        _save_firms(data)

    # Phase 3: Website scraping
    if not args.skip_websites:
        limit = 200 if args.test else None
        enrich_from_websites(firms, delay=0.5, limit=limit)
        data["firms"] = firms

    # Final save
    data["meta"]["lastScraped"] = datetime.now(timezone.utc).isoformat()
    _save_firms(data)

    after_website = sum(1 for f in firms if f.get("website"))
    after_phone = sum(1 for f in firms if f.get("phone"))
    after_email = sum(1 for f in firms if f.get("email"))
    print(f"\n{'='*50}")
    print(f"  Enrichment Complete")
    print(f"{'='*50}")
    print(f"  Websites: {before_website} → {after_website} (+{after_website - before_website})")
    print(f"  Phones:   {before_phone} → {after_phone} (+{after_phone - before_phone})")
    print(f"  Emails:   {before_email} → {after_email} (+{after_email - before_email})")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
