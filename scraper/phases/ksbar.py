import uuid, requests
from bs4 import BeautifulSoup
from scraper.utils.normalize import normalize_practice_area, are_same_firm

KSBAR_URL = "https://www.ksbar.org/search/members"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; LawFirmDirectory/1.0)"}


def is_js_rendered(html: str) -> bool:
    """Return True if the page is a JS-rendered shell with no member data."""
    soup = BeautifulSoup(html, "lxml")
    # Heuristic 1: fewer than 3 rows/items with substantial text content
    rows = soup.find_all(["tr", "li"])
    meaningful = [r for r in rows if len(r.get_text(strip=True)) > 10]
    if len(meaningful) >= 3:
        return False
    # Heuristic 2: body text too short to contain real directory data
    body_text = soup.get_text(strip=True)
    if len(body_text) < 2000:
        return True
    return False


def scrape_ksbar() -> list:
    try:
        resp = requests.get(KSBAR_URL, timeout=10, headers=HEADERS)
        resp.raise_for_status()
    except Exception as e:
        print(f"[ksbar] Request failed: {e}")
        return []

    if is_js_rendered(resp.text):
        print("[ksbar] WARNING: Page appears JS-rendered. Phase 2 skipped.")
        return []

    soup = BeautifulSoup(resp.text, "lxml")
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

        # Merge into existing entry for same firm in same city
        existing = next((e for e in entries if e["firmName"] == firm and e["city"] == city), None)
        if existing:
            for p in practices:
                if p not in existing["practiceAreas"]:
                    existing["practiceAreas"].append(p)
        else:
            entries.append({"firmName": firm, "practiceAreas": practices, "city": city})

    return entries


def merge_ksbar_into_firms(firms: list, ksbar_entries: list) -> list:
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
