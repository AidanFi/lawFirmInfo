"""
Phase 1: Kansas Supreme Court Attorney Registration Directory

Scrapes directory-kard.kscourts.org to find every licensed attorney in Kansas.
Uses Playwright to establish a Cloudflare-cleared session, searches A-Z to
collect all registration numbers, then fetches detail pages via requests.
"""
import re
import time
import uuid
import requests
from bs4 import BeautifulSoup
from scraper.utils.normalize import normalize_firm_name

SITE_URL = "https://directory-kard.kscourts.org"
SEARCH_URL = SITE_URL + "/"
DETAIL_URL = "https://directory-kard.kscourts.gov/Home/Details?regNum={}"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

_STATE_ZIP_RE = re.compile(r',?\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\s*$')
_LETTERS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")


def _establish_session():
    """Use Playwright to load the site and return cookies for requests."""
    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=USER_AGENT)
        page = ctx.new_page()
        page.goto(SEARCH_URL, timeout=20000, wait_until="domcontentloaded")
        time.sleep(2)
        cookies = ctx.cookies()
        browser.close()

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    for c in cookies:
        session.cookies.set(c["name"], c["value"], domain=c["domain"])
    return session


def _search_all_attorneys_playwright():
    """Search A-Z via Playwright and return dict of {regnum: (name, status)}."""
    from playwright.sync_api import sync_playwright

    attorneys = {}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=USER_AGENT)
        page = ctx.new_page()

        for letter in _LETTERS:
            page.goto(SEARCH_URL, timeout=20000, wait_until="domcontentloaded")
            time.sleep(1)
            page.fill("#LastName", letter)
            page.click('input[type="submit"], button[type="submit"]')
            time.sleep(3)

            content = page.content()
            parsed = _parse_search_results(content)
            new = 0
            for regnum, name, status in parsed:
                if regnum not in attorneys:
                    attorneys[regnum] = (name, status)
                    new += 1
            print(f"[ks_courts] Search '{letter}': {len(parsed)} results, {new} new (total unique: {len(attorneys)})")

        # Extract cookies for later requests use
        cookies = ctx.cookies()
        browser.close()

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    for c in cookies:
        session.cookies.set(c["name"], c["value"], domain=c["domain"])

    return attorneys, session


def _parse_search_results(html: str) -> list[tuple[str, str, str]]:
    """Parse search results table. Returns list of (regnum, name, status)."""
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return []

    results = []
    for row in table.find_all("tr")[1:]:  # skip header
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        name = cells[0].get_text(strip=True)
        status = cells[1].get_text(strip=True)
        regnum = cells[2].get_text(strip=True)
        if regnum:
            results.append((regnum, name, status))
    return results


def _parse_detail_page(html: str) -> dict | None:
    """Parse an attorney detail page for firm, address, and phone."""
    soup = BeautifulSoup(html, "lxml")
    fields = {}

    for row in soup.find_all("div", class_="row"):
        strong = row.find("strong")
        if not strong:
            continue
        label = strong.get_text(strip=True)
        value_div = row.find("div", class_="text-md-left")
        if not value_div:
            continue

        if label == "Business Mailing Address":
            paragraphs = value_div.find_all("p", class_="my-0")
            if paragraphs:
                lines = [p.get_text(strip=True) for p in paragraphs]
                fields["address_lines"] = lines
            else:
                text = value_div.get_text(separator="\n", strip=True)
                fields["address_lines"] = [l.strip() for l in text.split("\n") if l.strip()]
        else:
            fields[label] = value_div.get_text(strip=True)

    if not fields.get("Attorney Name"):
        return None

    return fields


def _extract_address_parts(address_lines: list[str]) -> tuple[str, str, str, str, str]:
    """Extract (firm_name, street, city, state, zip) from address lines.

    Typical format:
      Line 0: Firm Name (optional, if >2 lines)
      Line -1: City, ST ZIP
      Lines in between: street address
    """
    if not address_lines:
        return "", "", "", "KS", ""

    # Last line is typically "City, ST ZIP"
    last = address_lines[-1]
    city, state, zipcode = "", "KS", ""
    match = _STATE_ZIP_RE.search(last)
    if match:
        state = match.group(1)
        zipcode = match.group(2)
        city_part = last[:match.start()].strip().rstrip(",")
        # City might be preceded by other parts separated by comma
        parts = [p.strip() for p in city_part.split(",")]
        city = parts[-1] if parts else city_part
    else:
        city = last

    firm_name = ""
    street = ""

    if len(address_lines) == 1:
        pass  # Just city line
    elif len(address_lines) == 2:
        street = address_lines[0]
    elif len(address_lines) >= 3:
        firm_name = address_lines[0]
        street = ", ".join(address_lines[1:-1])

    return firm_name, street, city, state, zipcode


def _fetch_detail(session: requests.Session, regnum: str, delay: float = 1.0) -> dict | None:
    """Fetch and parse a detail page. Returns parsed fields or None."""
    url = DETAIL_URL.format(regnum)
    try:
        resp = session.get(url, timeout=10)
        if resp.status_code != 200:
            return None
        time.sleep(delay)
        return _parse_detail_page(resp.text)
    except Exception:
        return None


def scrape_ks_courts(start_from: int = 0, delay: float = 1.0, test_mode: bool = False) -> list:
    """Scrape the KS Supreme Court directory for all active Kansas attorneys.

    Returns a list of firm dicts grouped by firm name + city.
    """
    print("[ks_courts] Phase 1: Searching KS Supreme Court Attorney Directory...")
    print("[ks_courts] Step 1/3: Searching A-Z for all attorneys...")

    try:
        attorneys, session = _search_all_attorneys_playwright()
    except Exception as e:
        print(f"[ks_courts] WARNING: Playwright search failed: {e}")
        print("[ks_courts] Attempting direct detail page enumeration instead...")
        return _fallback_enumerate(start_from, delay, test_mode)

    # Filter to Active only
    active = {regnum: name for regnum, (name, status) in attorneys.items() if status == "Active"}
    print(f"[ks_courts] Found {len(attorneys)} total attorneys, {len(active)} active")

    if test_mode:
        # Limit to 100 for testing
        active = dict(list(active.items())[:100])
        print(f"[ks_courts] Test mode: limited to {len(active)} attorneys")

    # Fetch detail pages for active attorneys
    print(f"[ks_courts] Step 2/3: Fetching detail pages for {len(active)} active attorneys...")
    attorney_details = []
    regnums = sorted(active.keys(), key=int)

    for i, regnum in enumerate(regnums):
        if i < start_from:
            continue

        detail = _fetch_detail(session, regnum, delay=delay)
        if not detail:
            continue

        address_lines = detail.get("address_lines", [])
        firm_from_addr, street, city, state, zipcode = _extract_address_parts(address_lines)

        # Skip non-Kansas attorneys
        if state != "KS":
            continue

        attorney_details.append({
            "name": detail.get("Attorney Name", active[regnum]),
            "regnum": regnum,
            "firm_name": firm_from_addr,
            "phone": detail.get("Business Phone", ""),
            "street": street,
            "city": city,
            "state": state,
            "zip": zipcode,
        })

        if (i + 1) % 100 == 0:
            print(f"[ks_courts] Progress: {i + 1}/{len(regnums)} detail pages fetched, {len(attorney_details)} KS attorneys found")

    print(f"[ks_courts] Step 3/3: Grouping {len(attorney_details)} attorneys into firms...")
    firms = _group_attorneys_into_firms(attorney_details)
    print(f"[ks_courts] Result: {len(firms)} firms from {len(attorney_details)} active KS attorneys")
    return firms


def _fallback_enumerate(start_from: int, delay: float, test_mode: bool) -> list:
    """Fallback: enumerate reg numbers directly if Playwright search fails."""
    print("[ks_courts] Establishing session via Playwright...")
    try:
        session = _establish_session()
    except Exception as e:
        print(f"[ks_courts] ERROR: Could not establish session: {e}")
        return []

    max_num = 100 if test_mode else 35000
    attorney_details = []
    consecutive_misses = 0

    for regnum in range(max(1, start_from), max_num + 1):
        detail = _fetch_detail(session, str(regnum), delay=delay)
        if not detail:
            consecutive_misses += 1
            if consecutive_misses > 500:
                print(f"[ks_courts] 500 consecutive misses at regnum {regnum}, stopping enumeration")
                break
            continue

        consecutive_misses = 0
        status = detail.get("Current Status", "")
        if status != "Active":
            continue

        address_lines = detail.get("address_lines", [])
        firm_from_addr, street, city, state, zipcode = _extract_address_parts(address_lines)
        if state != "KS":
            continue

        attorney_details.append({
            "name": detail.get("Attorney Name", ""),
            "regnum": str(regnum),
            "firm_name": firm_from_addr,
            "phone": detail.get("Business Phone", ""),
            "street": street,
            "city": city,
            "state": state,
            "zip": zipcode,
        })

        if len(attorney_details) % 100 == 0:
            print(f"[ks_courts] Enumeration: regnum {regnum}, found {len(attorney_details)} active KS attorneys")

    firms = _group_attorneys_into_firms(attorney_details)
    print(f"[ks_courts] Result: {len(firms)} firms from {len(attorney_details)} active KS attorneys")
    return firms


def _group_attorneys_into_firms(attorney_details: list) -> list:
    """Group attorneys by normalized firm name + city into firm records."""
    groups = {}

    for atty in attorney_details:
        # Use firm name from address if available, otherwise use attorney name as solo practitioner
        firm_name = atty["firm_name"] if atty["firm_name"] else atty["name"]
        city = atty["city"]
        key = (normalize_firm_name(firm_name), city.lower())

        if key not in groups:
            groups[key] = {
                "name": firm_name,
                "attorneys": [],
                "phone": atty["phone"],
                "street": atty["street"],
                "city": city,
                "state": atty["state"],
                "zip": atty["zip"],
            }
        groups[key]["attorneys"].append(atty["name"])
        # Prefer phone from the firm record that has one
        if not groups[key]["phone"] and atty["phone"]:
            groups[key]["phone"] = atty["phone"]

    firms = []
    for group in groups.values():
        firms.append({
            "id": str(uuid.uuid4()),
            "name": group["name"],
            "practiceAreas": [],
            "summary": None,
            "website": None,
            "phone": group["phone"] or None,
            "email": None,
            "address": {
                "street": group["street"],
                "city": group["city"],
                "county": "",
                "state": group["state"],
                "zip": group["zip"],
            },
            "coordinates": None,
            "referralScore": "low",
            "sources": ["ks_courts"],
            "attorneys": group["attorneys"],
            "attorney_count": len(group["attorneys"]),
        })
    return firms
