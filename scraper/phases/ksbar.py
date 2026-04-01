"""
Phase 3: Kansas Bar Association Directory

Scrapes the KSBar member directory for practice area data and attorney-firm
associations. Uses Playwright as primary method (handles JS rendering),
falls back to static BeautifulSoup scraping if Playwright fails.
"""
import uuid
import requests
from bs4 import BeautifulSoup
from scraper.utils.normalize import normalize_practice_area, are_same_firm

KSBAR_URL = "https://www.ksbar.org/search/members"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; LawFirmDirectory/2.0)"}


def is_js_rendered(html: str) -> bool:
    """Return True if the page is a JS-rendered shell with no member data."""
    soup = BeautifulSoup(html, "lxml")
    rows = soup.find_all(["tr", "li"])
    meaningful = [r for r in rows if len(r.get_text(strip=True)) > 10]
    if len(meaningful) >= 3:
        return False
    body_text = soup.get_text(strip=True)
    if len(body_text) < 2000:
        return True
    return False


def _scrape_ksbar_static() -> list:
    """Original static HTML scraping approach."""
    try:
        resp = requests.get(KSBAR_URL, timeout=10, headers=HEADERS)
        resp.raise_for_status()
    except Exception as e:
        print(f"[ksbar] Static request failed: {e}")
        return []

    if is_js_rendered(resp.text):
        print("[ksbar] WARNING: Page appears JS-rendered. Static scrape returned no data.")
        return []

    return _parse_member_table(resp.text)


def _scrape_ksbar_playwright() -> list:
    """Use Playwright headless browser to scrape KSBar directory."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("[ksbar] Playwright not installed, skipping browser-based scrape")
        return []

    import time
    entries = []

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = ctx.new_page()
            page.goto(KSBAR_URL, timeout=20000, wait_until="domcontentloaded")
            time.sleep(5)  # Wait for JS rendering

            content = page.content()
            entries = _parse_member_table(content)

            if not entries:
                # Try navigating to alternative search URLs
                for alt_url in [
                    "https://www.ksbar.org/?pg=Members",
                    "https://www.ksbar.org/search/custom.asp?id=2870",
                ]:
                    page.goto(alt_url, timeout=20000, wait_until="domcontentloaded")
                    time.sleep(5)
                    content = page.content()
                    entries = _parse_member_table(content)
                    if entries:
                        break

            browser.close()
    except Exception as e:
        print(f"[ksbar] Playwright scrape failed: {e}")

    return entries


def _parse_member_table(html: str) -> list:
    """Parse member table from HTML. Returns list of {firmName, practiceAreas, city}."""
    soup = BeautifulSoup(html, "lxml")
    entries = []

    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 4:
            continue
        attorney_name = cells[0].get_text(strip=True)
        firm_name = cells[1].get_text(strip=True)
        raw_practices = cells[2].get_text(strip=True)
        city = cells[3].get_text(strip=True)

        if not attorney_name:
            continue

        firm = firm_name if firm_name else attorney_name
        practices = [normalize_practice_area(p.strip()) for p in raw_practices.split(",") if p.strip()]

        existing = next((e for e in entries if e["firmName"] == firm and e["city"] == city), None)
        if existing:
            for p in practices:
                if p not in existing["practiceAreas"]:
                    existing["practiceAreas"].append(p)
        else:
            entries.append({"firmName": firm, "practiceAreas": practices, "city": city})

    return entries


def scrape_ksbar() -> list:
    """Scrape KSBar directory. Tries Playwright first, falls back to static."""
    print("[ksbar] Attempting Playwright-based scrape...")
    entries = _scrape_ksbar_playwright()
    if entries:
        print(f"[ksbar] Playwright scrape: {len(entries)} entries")
        return entries

    print("[ksbar] Playwright returned no results, trying static HTML scrape...")
    entries = _scrape_ksbar_static()
    if entries:
        print(f"[ksbar] Static scrape: {len(entries)} entries")
    else:
        print("[ksbar] WARNING: No data from either method. KSBar phase skipped.")
    return entries


def merge_ksbar_into_firms(firms: list, ksbar_entries: list) -> list:
    """Merge KSBar entries into existing firm list."""
    updated = list(firms)
    for entry in ksbar_entries:
        match = next(
            (f for f in updated
             if f["address"]["city"].lower() == entry["city"].lower()
             and are_same_firm(f["name"], entry["firmName"])),
            None
        )
        if match:
            for p in entry["practiceAreas"]:
                if p not in match["practiceAreas"]:
                    match["practiceAreas"].append(p)
            if "ksbar" not in match["sources"]:
                match["sources"].append("ksbar")
        else:
            updated.append({
                "id": str(uuid.uuid4()),
                "name": entry["firmName"],
                "practiceAreas": entry["practiceAreas"],
                "summary": None,
                "website": None,
                "phone": None,
                "email": None,
                "address": {"street": "", "city": entry["city"], "county": "", "state": "KS", "zip": ""},
                "coordinates": None,
                "referralScore": "low",
                "sources": ["ksbar"],
            })
    return updated
