#!/usr/bin/env python3
"""
Pass 3 website enrichment for remaining KS provider orgs without websites.

Two-phase approach:
  Phase A: Enhanced domain guessing (fast, no rate limits) — new patterns:
           first word, word1+chiro/pt, name-minus-generic, acronym, etc.
  Phase B: Startpage in batches of 20 with 10-min cool-downs between batches.
           Fresh session per batch. Varied user agents. Proper timeout tuple.

Processes counties in HIGH-PRIORITY order (sedgwick first, then johnson, etc.)
so the freshest Startpage session hits the biggest counties.

Usage:
  python3 enrich_providers_v4.py          # all remaining orgs
  python3 enrich_providers_v4.py --phase-a-only   # domain guessing only
  python3 enrich_providers_v4.py --county sedgwick-county-ks
"""
import argparse
import csv
import re
import sys
import time
from pathlib import Path
from urllib.parse import quote_plus, urlparse

import requests

DATA_DIR = Path("app/county-data")

FIELDNAMES = [
    "provider_name", "website", "phone_number", "provider_type",
    "city", "state", "county", "street_address", "zip_code",
    "email", "npi_number",
]

_BAD_DOMAINS = frozenset({
    "healthgrades.com", "zocdoc.com", "vitals.com", "ratemds.com", "webmd.com",
    "doximity.com", "yelp.com", "yellowpages.com", "superpages.com", "whitepages.com",
    "facebook.com", "linkedin.com", "twitter.com", "instagram.com", "google.com",
    "bing.com", "wikipedia.org", "youtube.com", "bbb.org", "manta.com", "mapquest.com",
    "npiprofile.com", "npino.com", "npinumber.org", "npidb.org", "medicare.gov",
    "cms.gov", "npiregistry.cms.hhs.gov", "usnews.com", "castleconnolly.com",
    "sharecare.com", "duckduckgo.com", "startpage.com", "chirodirectory.com",
    "apta.org", "acatoday.org", "findachiropractor.com", "chiromatrix.com",
    "chiropractic.org", "doctor.com", "wellness.com", "healthline.com",
    "psychologytoday.com", "psychology-today.com", "birdeye.com",
    "practicefusion.com", "merchantcircle.com", "ezlocal.com", "showmelocal.com",
    "citysearch.com", "insiderpages.com", "chamberofcommerce.com", "angieslist.com",
    "homeadvisor.com", "thumbtack.com", "care.com", "therapyfinder.com",
    "therapist.com", "goodtherapy.org", "indeed.com", "glassdoor.com",
    "trustpilot.com", "foursquare.com", "wikidata.org", "npi.io", "nppes.com",
    "chiroeco.com", "chirotouch.com", "thechiropractors.com",
    "pt-helper.com", "pthelper.com",
})

_USER_AGENTS = [
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
     "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15"),
    ("Mozilla/5.0 (X11; Linux x86_64) "
     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"),
]

_ORG_RE = re.compile(
    r'\b(chiropractic|chiropractor|physical\s+therapy|therapy|therapist|center|clinic|'
    r'group|associates|health|wellness|rehab|sports|spine|family|care|institute|'
    r'services|practice|network|back|joint|llc|pllc|inc|pa|corp|acupuncture|'
    r'massage|orthopedic|medical|rehabilitation|movement|performance|solutions|'
    r'integrated|advanced|premier|optimal)\b', re.I
)

_GENERIC = frozenset({
    "chiropractic", "chiropractor", "therapy", "therapist", "therapies",
    "physical", "center", "clinic", "group", "health", "wellness", "rehab",
    "rehabilitation", "sports", "spine", "spinal", "family", "care", "institute",
    "services", "practice", "network", "acupuncture", "massage", "fitness",
    "orthopedic", "medical", "injury", "back", "pain", "movement", "motion",
    "performance", "manual", "balance", "core", "active", "integrated", "advanced",
    "premier", "elite", "professional", "optimal", "comprehensive", "county",
    "associates", "llc", "pllc", "inc", "pa", "corp", "ltd",
})


def _is_org(name: str) -> bool:
    return bool(_ORG_RE.search(name))


def _norm_domain(url: str) -> str:
    try:
        return re.sub(r"^www\.", "", urlparse(url.strip()).netloc.lower())
    except Exception:
        return ""


def _is_bad(url: str) -> bool:
    if not url or not url.startswith("http"):
        return True
    if re.search(r"\.(pdf|zip|doc|docx|xls|xlsx|csv|jpg|png|gif|mp4|mp3|ppt|pptx)(\?|$)", url, re.I):
        return True
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if host.endswith(".gov") or host.endswith(".edu"):
        return True
    if re.search(r"\bci\.[a-z]+\.[a-z]{2}\.us$", host):
        return True
    if host.endswith(".us") and any(x in host for x in ("city.", "county.", "town.", ".ci.", ".co.")):
        return True
    path = parsed.path.lower()
    if "/sites/default/files/" in path or "/fileattachments/" in path:
        return True
    d = _norm_domain(url)
    return any(d == b or d.endswith("." + b) for b in _BAD_DOMAINS)


def _clean_name(name: str) -> str:
    out = re.sub(r",?\s*\b(llc|pllc|inc|pa|ltd|corp|dba)\b", "", name, flags=re.I)
    out = re.sub(r",?\s*\b(d\.?c\.?|dpt|pt|lpt|mspt|mpt|dc|md)\b", "", out, flags=re.I)
    return re.sub(r"\s+", " ", out).strip().strip(",").strip()


def _head_resolves(url: str, ua: str) -> bool:
    try:
        r = requests.head(url, timeout=(5, 8), allow_redirects=True, verify=False,
                          headers={"User-Agent": ua})
        return r.status_code < 400
    except Exception:
        return False


_HEALTH_SUFFIXES = [
    "chiropractic", "chiro", "pt", "physicaltherapy", "therapy",
    "spine", "rehab", "rehabilitation", "wellness", "health",
]


def _domain_candidates(name: str, city: str) -> list[str]:
    """
    High-precision domain candidates only.

    Only tries patterns that combine a distinctive name word with a health suffix
    or city name. This avoids the false-positive trap of standalone generic-word
    domains (rule.com, grace.com, able.com, weber.com etc.).

    Does NOT try:
      - Full name slugs (too generic when name has common words)
      - Stripped-name slugs (standalone "rule" → rule.com)
      - Acronyms (2-4 letter initials hit unrelated squatters: so.com, ca.com)
    """
    cleaned = _clean_name(name)
    city_slug = re.sub(r"[^a-z0-9]", "", city.lower())

    # Distinctive words: >= 5 chars, not generic
    words = [w for w in re.split(r"\W+", cleaned.lower()) if len(w) >= 5 and w not in _GENERIC]

    candidates = []

    if not words:
        return candidates

    w0 = words[0]  # most distinctive word

    # Pattern A: distinctive_word + health_suffix  (HIGH PRECISION)
    for suffix in _HEALTH_SUFFIXES:
        candidates.append(f"https://{w0}{suffix}.com")
        if suffix in ("chiro", "pt"):
            candidates.append(f"https://www.{w0}{suffix}.com")

    # Pattern B: distinctive_word + city  (HIGH PRECISION)
    if city_slug and len(city_slug) >= 4:
        candidates.append(f"https://{w0}{city_slug}.com")
        candidates.append(f"https://{city_slug}{w0}.com")

    # Pattern C: all distinctive words joined + health suffix (e.g. "bergkampchiro.com")
    if len(words) >= 2:
        combined = "".join(words[:2])
        if len(combined) >= 8:
            candidates.append(f"https://{combined}.com")
            candidates.append(f"https://{combined}chiro.com")
            candidates.append(f"https://{combined}pt.com")

    # Pattern D: full name slug — ONLY if it's long enough to be specific (>= 14 chars)
    # and it naturally includes a health word (e.g. "pomeroychiropracticwellness")
    base = re.sub(r"[^a-z0-9]", "", cleaned.lower())
    if len(base) >= 14:
        candidates.append(f"https://{base}.com")

    # Deduplicate preserving order
    seen: set[str] = set()
    result = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            result.append(c)
    return result


def _domain_part_len(url: str) -> int:
    try:
        d = re.sub(r"^www\.", "", urlparse(url.strip()).netloc.lower())
        return len(d.rsplit(".", 1)[0])
    except Exception:
        return 0


def pass_domain_guess(records: list[dict]) -> int:
    ua = _USER_AGENTS[0]
    targets = [r for r in records if not r.get("website", "").strip() and _is_org(r.get("provider_name", ""))]
    print(f"  Enhanced domain guessing: {len(targets)} orgs...", flush=True)
    found = 0
    for rec in targets:
        for url in _domain_candidates(rec["provider_name"], rec.get("city", "")):
            # Require domain part >= 6 chars to avoid short acronym false positives
            if _domain_part_len(url) < 6:
                continue
            if _head_resolves(url, ua):
                rec["website"] = url
                found += 1
                print(f"    ✓ {rec['provider_name'][:50]} → {url}", flush=True)
                break
        time.sleep(0.1)
    print(f"    Found: {found}", flush=True)
    return found


def share_address_phone(records: list[dict]) -> int:
    addr_to_web: dict[tuple, str] = {}
    phone_to_web: dict[str, str] = {}
    for r in records:
        web = r.get("website", "").strip()
        if not web:
            continue
        addr = re.sub(r"\s+", " ", (r.get("street_address", "") or "").lower().strip())
        zip_ = (r.get("zip_code", "") or "")[:5]
        if addr and zip_ and len(addr) > 5:
            addr_to_web[(addr, zip_)] = web
        ph = re.sub(r"[^\d]", "", r.get("phone_number", "") or "")
        if len(ph) == 10:
            phone_to_web[ph] = web

    found = 0
    for r in records:
        if r.get("website", "").strip():
            continue
        addr = re.sub(r"\s+", " ", (r.get("street_address", "") or "").lower().strip())
        zip_ = (r.get("zip_code", "") or "")[:5]
        web = addr_to_web.get((addr, zip_), "")
        if not web:
            ph = re.sub(r"[^\d]", "", r.get("phone_number", "") or "")
            web = phone_to_web.get(ph, "") if len(ph) == 10 else ""
        if web:
            r["website"] = web
            found += 1
    return found


_GENERIC_WORDS = frozenset({
    "chiropractic", "chiropractor", "therapy", "therapist", "physical",
    "center", "clinic", "group", "health", "wellness", "rehab", "rehabilitation",
    "sports", "spine", "family", "care", "institute", "services", "practice",
    "associates", "back", "joint", "pain", "injury", "advanced", "premier",
    "optimal", "integrated", "medical", "orthopedic", "acupuncture", "massage",
    "performance", "movement", "motion", "balance", "core", "active",
})


def _domain_has_name_hint(url: str, name: str, city: str) -> bool:
    parsed = urlparse(url)
    domain = re.sub(r"^www\.", "", parsed.netloc.lower()).replace("-", "").replace(".", "")
    full_url_lower = url.lower()
    cleaned = _clean_name(name).lower()
    words = [w for w in re.split(r"\W+", cleaned) if len(w) >= 4 and w not in _GENERIC_WORDS]
    city_slug = re.sub(r"[^a-z0-9]", "", city.lower())
    if any(w in domain for w in words):
        return True
    if len(city_slug) >= 4 and city_slug in domain:
        return True
    for w in words:
        if w in full_url_lower:
            return True
    return False


def _make_session(ua_idx: int) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": _USER_AGENTS[ua_idx % len(_USER_AGENTS)],
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return s


def startpage_search(session: requests.Session, name: str, city: str, state: str, ptype: str) -> str:
    cleaned = _clean_name(name)
    if not cleaned:
        return ""
    query = f"{cleaned} {ptype} {city} {state}"
    url = f"https://www.startpage.com/sp/search?q={quote_plus(query)}&language=english"
    try:
        r = session.get(url, timeout=(8, 18))  # separate connect vs read timeout
        if r.status_code != 200:
            return ""
        candidates = []
        for href in re.findall(r'href=["\']?(https?://[^"\'> &]+)', r.text):
            if "startpage.com" in href:
                continue
            href = href.replace("&amp;", "&")
            if not _is_bad(href):
                candidates.append(href)
        if not candidates:
            return ""
        for href in candidates:
            if _domain_has_name_hint(href, name, city):
                return href
        first = candidates[0]
        domain = _norm_domain(first)
        if re.search(r"(chiro|pt|therapy|health|wellness|spine|rehab|clinic|care|sport|physio)", domain, re.I):
            return first
        return ""
    except Exception:
        return ""


def pass_startpage_batched(records: list[dict], batch_size: int = 20, cooldown: int = 600) -> int:
    """Startpage search in batches with cool-down between batches."""
    targets = [r for r in records if not r.get("website", "").strip() and _is_org(r.get("provider_name", ""))]
    print(f"  Startpage (batched): {len(targets)} orgs, batch_size={batch_size}, cooldown={cooldown}s...", flush=True)
    found = 0
    ua_idx = 0

    for batch_start in range(0, len(targets), batch_size):
        batch = targets[batch_start:batch_start + batch_size]
        session = _make_session(ua_idx)
        ua_idx += 1

        batch_end = batch_start + len(batch)
        print(f"    Batch {batch_start+1}-{batch_end}/{len(targets)} (UA #{ua_idx})", flush=True)

        for i, rec in enumerate(batch):
            site = startpage_search(session, rec.get("provider_name", ""),
                                    rec.get("city", ""), rec.get("state", "KS"),
                                    rec.get("provider_type", "").lower())
            if site:
                rec["website"] = site
                found += 1
                print(f"      ✓ {rec['provider_name'][:50]} → {site}", flush=True)
            time.sleep(20)  # 20s between queries within a batch

        print(f"    Batch done: {found} total found so far", flush=True)
        session.close()

        if batch_end < len(targets):
            print(f"    Cooling down {cooldown}s ({cooldown//60} min)...", flush=True)
            time.sleep(cooldown)

    print(f"  Startpage total: {found}", flush=True)
    return found


def read_csv(path: Path) -> list[dict]:
    return list(csv.DictReader(path.open()))


def write_csv(path: Path, records: list[dict]):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        w.writeheader()
        w.writerows(records)


def get_priority_order() -> list[Path]:
    """Return county CSVs sorted by number of remaining orgs (highest first)."""
    all_csvs = list(DATA_DIR.glob("providers-*-ks.csv"))
    priority = []
    for p in all_csvs:
        rows = read_csv(p)
        n_orgs = sum(1 for r in rows if not r.get("website", "").strip() and _is_org(r.get("provider_name", "")))
        priority.append((n_orgs, p))
    priority.sort(key=lambda x: -x[0])
    return [p for _, p in priority if _ > 0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase-a-only", action="store_true", help="Only run domain guessing")
    parser.add_argument("--county", help="Process only this county slug")
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--cooldown", type=int, default=600, help="Cooldown between batches (seconds)")
    args = parser.parse_args()

    if args.county:
        slug = args.county
        matches = list(DATA_DIR.glob(f"*{slug}*.csv"))
        if not matches:
            print(f"No CSV found for {args.county}")
            sys.exit(1)
        ordered = matches
    else:
        ordered = get_priority_order()

    print(f"Processing {len(ordered)} counties (highest org-count first)\n", flush=True)

    total_domain = 0
    total_startpage = 0
    total_shared = 0

    for idx, csv_path in enumerate(ordered, 1):
        records = read_csv(csv_path)
        before = sum(1 for r in records if r.get("website", "").strip())
        n_orgs = sum(1 for r in records if not r.get("website", "").strip() and _is_org(r.get("provider_name", "")))
        if n_orgs == 0:
            continue

        print(f"\n[{idx}] {csv_path.stem} — {len(records)} providers, {before} with website, {n_orgs} orgs need search", flush=True)

        # Phase A: enhanced domain guessing
        d = pass_domain_guess(records)
        total_domain += d

        # Share after domain guessing
        s1 = share_address_phone(records)
        total_shared += s1

        # Write intermediate
        write_csv(csv_path, records)

        # Phase B: Startpage (unless skipped)
        if not args.phase_a_only:
            sp = pass_startpage_batched(records, args.batch_size, args.cooldown)
            total_startpage += sp

            # Share again after Startpage
            s2 = share_address_phone(records)
            total_shared += s2

        # Write final
        write_csv(csv_path, records)

        after = sum(1 for r in records if r.get("website", "").strip())
        print(f"  {csv_path.stem}: {before} → {after} websites", flush=True)

    # Global sharing pass
    print("\n=== Global sharing pass ===", flush=True)
    all_csvs = sorted(DATA_DIR.glob("providers-*-ks.csv"))
    all_records: list[dict] = []
    file_slices: list[tuple[Path, list[dict]]] = []
    for p in all_csvs:
        rows = read_csv(p)
        file_slices.append((p, rows))
        all_records.extend(rows)

    global_shared = share_address_phone(all_records)
    total_shared += global_shared
    print(f"  Global sharing: +{global_shared}", flush=True)

    for p, rows in file_slices:
        write_csv(p, rows)

    # Final stats
    print("\n=== Summary ===")
    print(f"Domain guessing:     +{total_domain}")
    print(f"Startpage:           +{total_startpage}")
    print(f"Address/phone share: +{total_shared}")

    total = sum(len(read_csv(p)) for p in all_csvs)
    with_web = sum(sum(1 for r in read_csv(p) if r.get("website", "").strip()) for p in all_csvs)
    print(f"\nFinal: {with_web}/{total} ({100*with_web/total:.1f}%) providers have website")


if __name__ == "__main__":
    main()
