import re, time
import requests
from bs4 import BeautifulSoup

SKIP_EMAIL_PATTERNS = re.compile(r'noreply|no-reply|admin@|webmaster@', re.IGNORECASE)
# Note: info@ is kept — many small law firms only publish info@ as their contact email
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; LawFirmDirectory/1.0)"}

# Practice area keyword detection — maps canonical area → list of trigger phrases
_PRACTICE_KEYWORDS = {
    "Personal Injury":           ["personal injury", "car accident", "auto accident", "slip and fall",
                                   "wrongful death", "injury attorney", "injury lawyer", "accident attorney"],
    "Family Law":                ["family law", "divorce", "child custody", "child support",
                                   "adoption", "prenuptial", "family attorney", "family lawyer"],
    "Criminal Defense":          ["criminal defense", "criminal law", "felony", "misdemeanor",
                                   "dui defense", "dwi defense", "criminal attorney", "criminal lawyer"],
    "DUI":                       ["dui", "dwi", "drunk driving", "driving under the influence"],
    "Estate Planning":           ["estate planning", "wills and trusts", "living trust", "probate",
                                   "estate attorney", "power of attorney", "elder law"],
    "Workers' Compensation":     ["workers compensation", "workers' compensation", "work injury",
                                   "workplace injury", "workers comp"],
    "Bankruptcy":                ["bankruptcy", "chapter 7", "chapter 13", "debt relief", "debt attorney"],
    "Business Law":              ["business law", "corporate law", "business attorney", "contracts",
                                   "llc formation", "business litigation", "commercial law"],
    "Real Estate":               ["real estate", "property law", "real estate attorney",
                                   "title insurance", "real estate lawyer", "property attorney"],
    "Immigration":               ["immigration", "visa", "green card", "citizenship",
                                   "deportation", "immigration attorney", "immigration lawyer"],
    "Employment Law":            ["employment law", "wrongful termination", "discrimination",
                                   "workplace harassment", "eeoc", "employment attorney"],
    "Medical Malpractice":       ["medical malpractice", "medical negligence", "hospital negligence",
                                   "doctor negligence", "nursing home"],
    "Social Security Disability":["social security", "disability benefits", "ssdi", "ssi",
                                   "disability attorney", "disability lawyer"],
    "Civil Litigation":          ["civil litigation", "civil trial", "civil dispute", "civil attorney"],
    "Intellectual Property":     ["intellectual property", "trademark", "patent", "copyright"],
    "Tax Law":                   ["tax law", "tax attorney", "irs", "tax litigation", "tax relief"],
}


def _extract_practice_areas(text: str) -> list[str]:
    """Scan page text for practice area keywords and return matched canonical areas."""
    lower = text.lower()
    found = []
    for area, keywords in _PRACTICE_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            found.append(area)
    return found


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
    result = {"summary": None, "email": None, "practiceAreas": []}
    try:
        resp = requests.get(url, timeout=5, headers=HEADERS)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        result["summary"] = _extract_summary(soup, name, city)
        result["email"] = _extract_email(soup)
        page_text = soup.get_text(separator=" ", strip=True)
        result["practiceAreas"] = _extract_practice_areas(page_text)
        if result["email"] is None:
            contact_soup = _fetch_contact_page(url)
            if contact_soup:
                result["email"] = _extract_email(contact_soup)
    except Exception:
        result["summary"] = f"{name} — law firm in {city}, Kansas"
    return result
