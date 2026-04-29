"""
Avvo Directory Discovery for County-Level Scraping

Uses Playwright to bypass Cloudflare, then requests for fast pagination.
Discovers attorneys and firms from Avvo's city-level directory pages.
"""
import re
import time
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

WYANDOTTE_ZIPS = {
    "66101", "66102", "66103", "66104", "66105", "66106",
    "66109", "66111", "66112", "66115", "66117", "66118",
    "66012", "66113",
}

_ZIP_FROM_URL = re.compile(r"/attorneys/(\d{5})-")
_PHONE_RE = re.compile(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")


def _establish_session(city_url: str) -> tuple[requests.Session, int]:
    """Load initial page via Playwright, return session with cookies and page count."""
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=UA)
        page = ctx.new_page()
        page.goto(city_url, timeout=30000, wait_until="domcontentloaded")
        time.sleep(5)

        content = page.content()
        cookies = ctx.cookies()
        browser.close()

    session = requests.Session()
    session.headers["User-Agent"] = UA
    session.headers["Accept"] = "text/html,application/xhtml+xml"
    for c in cookies:
        session.cookies.set(c["name"], c["value"], domain=c["domain"])

    soup = BeautifulSoup(content, "html.parser")
    max_page = _get_max_page(soup)

    return session, max_page, content


def _get_max_page(soup: BeautifulSoup) -> int:
    """Extract max page number from pagination links."""
    max_page = 1
    for link in soup.find_all("a", href=re.compile(r"page=\d+")):
        text = link.get_text(strip=True)
        if text.isdigit():
            max_page = max(max_page, int(text))
    return max_page


def _parse_page(html: str) -> list[dict]:
    """Parse attorney cards from a page. Returns list of attorney dicts."""
    soup = BeautifulSoup(html, "html.parser")
    attorneys = []

    profile_links = soup.find_all("a", href=re.compile(r"/attorneys/\d{5}-"))
    seen_urls = set()

    for link in profile_links:
        href = link.get("href", "")
        if href in seen_urls:
            continue

        name = link.get_text(strip=True)
        if not name or len(name) < 3 or name in ("View Profile", ""):
            continue
        if "review" in name.lower() or name[0].isdigit():
            continue

        seen_urls.add(href)
        zip_match = _ZIP_FROM_URL.search(href)
        zip_code = zip_match.group(1) if zip_match else ""

        parent = link
        for _ in range(8):
            parent = parent.parent
            if parent is None or parent.name == "body":
                break
            classes = " ".join(parent.get("class", []))
            if "lawyer" in classes.lower() or "serp" in classes.lower() or "card" in classes.lower():
                break

        card_text = parent.get_text(separator=" | ", strip=True) if parent else ""

        firm_name = ""
        practice_areas = []
        phone = ""
        address = ""

        if parent and parent.name != "body":
            practice_match = re.search(
                r"Practice Areas?:\s*\n?\s*(.+?)(?:\||Bio:|Rating:|$)",
                card_text
            )
            if practice_match:
                raw_areas = practice_match.group(1).strip()
                practice_areas = [a.strip() for a in re.split(r",\s*| and more", raw_areas) if a.strip()]

            phone_match = _PHONE_RE.search(card_text)
            if phone_match:
                phone = phone_match.group()

            parts = card_text.split("|")
            for part in parts:
                part = part.strip()
                if "Kansas City, KS" in part or "Bonner Springs, KS" in part:
                    if "," in part and any(c.isdigit() for c in part):
                        address = part
                    elif not address:
                        address = part

            for part in parts:
                part = part.strip()
                if (part and part != name and "review" not in part.lower()
                        and "rating" not in part.lower() and "licensed" not in part.lower()
                        and "practice" not in part.lower() and "bio:" not in part.lower()
                        and not part[0].isdigit() and "save" not in part.lower()
                        and "call" not in part.lower() and "profile" not in part.lower()
                        and "chat" not in part.lower() and "message" not in part.lower()
                        and "PRO" != part and "View" not in part
                        and len(part) > 3 and len(part) < 80
                        and ("law" in part.lower() or "llc" in part.lower()
                             or "pa" in part.lower() or "p.a" in part.lower()
                             or "firm" in part.lower() or "office" in part.lower()
                             or "assoc" in part.lower() or "&" in part
                             or "," in part and not any(c.isdigit() for c in part))):
                    if part != name:
                        firm_name = part
                        break

        attorneys.append({
            "name": name,
            "firm_name": firm_name,
            "zip_code": zip_code,
            "practice_areas": practice_areas,
            "phone": phone,
            "address": address,
            "profile_url": href if href.startswith("http") else f"https://www.avvo.com{href}",
        })

    return attorneys


def discover_avvo(county_config: dict, test_mode: bool = False) -> list[dict]:
    """Discover attorneys from Avvo for all cities in a county config."""
    cities = county_config["cities"]
    state = county_config["state"].lower()
    county_zips = set(county_config.get("zip_codes", []))

    all_attorneys = []

    for city in cities:
        city_slug = city.lower().replace(" ", "-")
        base_url = f"https://www.avvo.com/all-lawyers/{state}/{city_slug}.html"

        print(f"  [avvo] Establishing session for {city}...")
        try:
            session, max_page, first_page_html = _establish_session(base_url)
        except Exception as e:
            print(f"  [avvo] Failed to load {city}: {e}")
            continue

        if test_mode:
            max_page = min(max_page, 3)

        print(f"  [avvo] {city}: {max_page} pages to scrape")

        page_attorneys = _parse_page(first_page_html)
        all_attorneys.extend(page_attorneys)

        for page_num in range(2, max_page + 1):
            url = f"{base_url}?page={page_num}"
            try:
                resp = session.get(url, timeout=15)
                if resp.status_code != 200:
                    print(f"  [avvo] Page {page_num} returned {resp.status_code}, stopping")
                    break
                if "Just a moment" in resp.text:
                    print(f"  [avvo] Cloudflare block at page {page_num}, stopping")
                    break
            except requests.RequestException as e:
                print(f"  [avvo] Request error at page {page_num}: {e}")
                break

            page_attorneys = _parse_page(resp.text)
            all_attorneys.extend(page_attorneys)

            if page_num % 25 == 0:
                print(f"  [avvo] Progress: page {page_num}/{max_page}, {len(all_attorneys)} attorneys so far")

            time.sleep(0.5)

    print(f"  [avvo] Total raw attorneys found: {len(all_attorneys)}")

    in_county = [a for a in all_attorneys if a["zip_code"] in county_zips]
    print(f"  [avvo] In-county (by zip): {len(in_county)}")

    return in_county


if __name__ == "__main__":
    from scraper.county.config import get_county_config
    config = get_county_config("wyandotte")
    results = discover_avvo(config, test_mode=True)
    print(f"\nTest results: {len(results)} attorneys in Wyandotte County")
    for a in results[:10]:
        print(f"  {a['name']} | firm={a['firm_name']} | zip={a['zip_code']} | areas={a['practice_areas']}")
