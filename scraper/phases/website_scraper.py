import re, time
import requests
from bs4 import BeautifulSoup

SKIP_EMAIL_PATTERNS = re.compile(r'noreply|no-reply|admin@|webmaster@', re.IGNORECASE)
# Note: info@ is kept — many small law firms only publish info@ as their contact email
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; LawFirmDirectory/1.0)"}


def _extract_summary(soup: BeautifulSoup, name: str, city: str) -> str:
    meta = soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content", "").strip():
        return meta["content"].strip()[:250]

    og = soup.find("meta", attrs={"property": "og:description"})
    if og and og.get("content", "").strip():
        return og["content"].strip()[:250]

    for container in [soup.find("main"), soup.find("article"), soup.body]:
        if not container:
            continue
        for p in container.find_all("p"):
            text = p.get_text(separator=" ", strip=True)
            if len(text) > 60:
                return text[:250]

    return f"{name} — law firm in {city}, Kansas"


def _extract_email(soup: BeautifulSoup) -> str | None:
    emails = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("mailto:"):
            addr = href[7:].split("?")[0].strip()
            if addr and not SKIP_EMAIL_PATTERNS.search(addr):
                emails.append(addr)
    return emails[0] if emails else None


def _fetch_contact_page(base_url: str) -> BeautifulSoup | None:
    for path in ["/contact", "/contact-us"]:
        try:
            r = requests.get(base_url.rstrip("/") + path, timeout=5, headers=HEADERS)
            r.raise_for_status()
            return BeautifulSoup(r.text, "lxml")
        except Exception:
            pass
    return None


def scrape_firm_website(url: str, name: str, city: str) -> dict:
    result = {"summary": None, "email": None}
    try:
        resp = requests.get(url, timeout=5, headers=HEADERS)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        result["summary"] = _extract_summary(soup, name, city)
        result["email"] = _extract_email(soup)
        if result["email"] is None:
            contact_soup = _fetch_contact_page(url)
            if contact_soup:
                result["email"] = _extract_email(contact_soup)
    except Exception:
        result["summary"] = f"{name} — law firm in {city}, Kansas"
    return result
