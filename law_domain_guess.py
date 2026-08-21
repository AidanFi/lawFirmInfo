#!/usr/bin/env python3
"""
High-precision domain-guessing website discovery for solo/small law practices.

DuckDuckGo is unreachable from this environment (confirmed via direct network
test — connection timeout, not "no results"), and raw Bing/Brave HTML scraping
returns low-quality/irrelevant matches without an API key. Small-town solo
attorneys very often just register firstnamelastname.com or lastnamelaw.com —
this guesses those patterns directly and validates by fetching real page
content (not just an HTTP 200), requiring the page actually mention the
attorney/firm name and a law-practice term. This is the same technique already
proven for provider (chiro/PT) website discovery in enrich_providers_v4.py.
"""
import re
import time
from urllib.parse import urlparse

import requests
import warnings
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
}

_STRIP_RE = re.compile(
    r",?\s*\b(llc|pllc|plc|pa|p\.a\.|pc|p\.c\.|llp|ltd|corp|chtd|chartered|"
    r"law\s+office(s)?|law\s+firm|attorney(s)?\s+at\s+law|and\s+associates|"
    r"&\s+associates|associates|esq|of\s+counsel)\b",
    re.IGNORECASE,
)

_GENERIC_WORDS = frozenset({
    "law", "legal", "office", "offices", "firm", "group", "attorney", "attorneys",
    "and", "the", "of", "at", "for", "county", "general", "practice",
})

_BAD_HOST_SUFFIXES = (
    "facebook.com", "linkedin.com", "avvo.com", "findlaw.com", "justia.com",
    "martindale.com", "lawyer.com", "lawyers.com", "yelp.com", "yellowpages.com",
    "mapquest.com", "bbb.org", "manta.com", "birdeye.com", "nextdoor.com",
    "yahoo.com", "kscourts.gov",
)


def _clean_name(name: str) -> str:
    out = _STRIP_RE.sub("", name)
    out = re.sub(r"[^a-zA-Z\s]", " ", out)
    return re.sub(r"\s+", " ", out).strip()


def _tokens(name: str) -> list[str]:
    cleaned = _clean_name(name)
    return [w.lower() for w in cleaned.split() if w.lower() not in _GENERIC_WORDS and len(w) >= 2]


def domain_candidates(firm_name: str) -> list[str]:
    """Generate high-precision domain guesses for a solo/small law practice."""
    words = _tokens(firm_name)
    if not words:
        return []

    candidates = []

    if len(words) >= 2:
        first, last = words[0], words[-1]
        joined = first + last
        if len(joined) >= 8:
            candidates.append(f"https://www.{joined}.com")
        candidates.append(f"https://www.{last}law.com")
        candidates.append(f"https://www.{last}lawoffice.com")
        candidates.append(f"https://www.{last}lawfirm.com")
        candidates.append(f"https://www.{first}{last}law.com")
        candidates.append(f"https://www.{last}attorney.com")
        candidates.append(f"https://www.{last}legal.com")
    else:
        w0 = words[0]
        if len(w0) >= 8:
            candidates.append(f"https://www.{w0}.com")
        candidates.append(f"https://www.{w0}law.com")
        candidates.append(f"https://www.{w0}lawoffice.com")
        candidates.append(f"https://www.{w0}legal.com")

    # Two-surname firms ("Newton and Forsyth" -> [newton, forsyth])
    if len(words) == 2:
        a, b = words
        candidates.append(f"https://www.{a}{b}.com")
        candidates.append(f"https://www.{a}{b}law.com")
        candidates.append(f"https://www.{b}{a}law.com")

    # dedupe, preserve order
    seen = set()
    out = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def _host_is_directory(url: str) -> bool:
    host = urlparse(url).netloc.lower().lstrip("www.")
    return any(host == b or host.endswith("." + b) for b in _BAD_HOST_SUFFIXES)


def _location_tokens(city: str, state: str, zip_code: str, phone: str) -> list[str]:
    """Only strong, low-collision signals. A bare 3-digit area code is NOT
    included — it produces false positives (matches random hex/hash strings,
    CSS colors, years, etc. in page source)."""
    toks = []
    if city and len(city.strip()) >= 4:
        toks.append(city.lower().strip())
    if zip_code and re.fullmatch(r"\d{5}", zip_code.strip()):
        toks.append(zip_code.strip())
    if phone:
        digits = re.sub(r"\D", "", phone)
        if len(digits) >= 10:
            local_number = digits[-7:]  # 7-digit local number, low collision risk
            toks.append(local_number)
    return [t for t in toks if t]


def validate_candidate(url: str, firm_name: str, city: str = "", state: str = "",
                        zip_code: str = "", phone: str = "", timeout: int = 6) -> bool:
    """Fetch the page and require it mentions the firm/attorney, a law-practice
    term, AND a location signal (city/zip/area code) — a bare name+law-term
    match is not enough for common surnames (e.g. "Esparza Law Office" and
    "Westbrook Law" both exist as unrelated firms in other states)."""
    if _host_is_directory(url):
        return False
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, verify=False, allow_redirects=True)
        if r.status_code >= 400:
            return False
        text = r.text.lower()
    except Exception:
        return False

    has_law_term = any(kw in text for kw in [
        "attorney", "lawyer", "law office", "law firm", "esq.", "legal services",
        "practice of law", "counselor at law",
    ])
    if not has_law_term:
        return False

    # Require at least one distinctive name token to appear on the page
    words = [w for w in _tokens(firm_name) if len(w) >= 4]
    if words and not any(w in text for w in words):
        return False

    # Require a location signal — this is what actually distinguishes the real
    # local firm from a same-named firm/squatter in a different state. Phone
    # match requires the digits appear together with only light separators
    # (not a bare substring of page digits, which false-positives on
    # minified JS/CSS hex strings).
    loc_tokens = _location_tokens(city, state, zip_code, phone)
    matched = False
    for t in loc_tokens:
        if t.isdigit() and len(t) == 7:
            phone_re = re.compile(re.escape(t[:3]) + r"[\s\-.]{0,2}" + re.escape(t[3:]))
            if phone_re.search(text):
                matched = True
                break
        elif t in text:
            matched = True
            break
    if loc_tokens and not matched:
        return False

    return True


def find_website_by_guessing(firm_name: str, city: str = "", state: str = "",
                              zip_code: str = "", phone: str = "") -> str | None:
    for url in domain_candidates(firm_name):
        if validate_candidate(url, firm_name, city, state, zip_code, phone):
            return url
        time.sleep(0.3)
    return None


if __name__ == "__main__":
    import sys
    for name in sys.argv[1:]:
        candidates = domain_candidates(name)
        print(f"\n{name}")
        print(f"  candidates: {candidates}")
        found = find_website_by_guessing(name)
        print(f"  RESULT: {found}")
