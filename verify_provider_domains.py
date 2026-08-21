#!/usr/bin/env python3
"""
Post-domain-guessing verification pass.

Removes false positives from provider CSVs:
  1. Domain part < 6 chars (catches short acronyms: so.com, ca.com, jcc.com etc.)
  2. Known-bad / brand domains (bodyshop.net, kirby.com, frontline.com etc.)
  3. Content verification: GET homepage, must contain chiro/PT keywords.
     Skips content check for clearly-good patterns (name word in domain).

After removal, re-runs address/phone sharing.
"""
import csv
import re
import time
from pathlib import Path
from urllib.parse import urlparse

import requests
import warnings
warnings.filterwarnings("ignore")

DATA_DIR = Path("app/county-data")

FIELDNAMES = [
    "provider_name", "website", "phone_number", "provider_type",
    "city", "state", "county", "street_address", "zip_code",
    "email", "npi_number",
]

# Domains we know are not chiro/PT practices
_BRAND_DOMAINS = frozenset({
    "bodyshop.net", "bodyshop.com",
    "frontline.com", "pbs.org",
    "kirby.com",
    "weber.com",
    "summit.com",
    "grace.com",
    "ivy.com",
    "able.com",
    "rule.com",
    "peters.com",
    "one.com",
    "orthopedic.com",
    "southeast.com",
    "howell.net",
    "dodson.com",
    "bergkamp.com",
    "tabor.com",
    "huynh.com",
    "goza.net",
    "ewert.com",
    "backtolife.com",
    "greenleaf.com",
    "skycity.com",
    "northrock.com",
    "shubert.com",
    "evolve.com",
    "wilsonand.com",
    "andhealing.com",
    "gouldtherapy.com",   # gouldtherapy.com is a therapy site but not likely this practice
    "thewichita.com",
    "preferredmedical.com",  # generic
    "michaelhealth.com",     # too generic
    "elizabeththerapy.com",  # individual PT
    "matthewchiropractic.com",  # generic first name
    "dennischiro.com",          # generic first name
    "markchiro.com",            # generic first name
    "mattchiro.com",            # generic first name
    "ryanchiropractic.com",     # generic first name
    "allenchiro.com",           # generic first name
    "centralchiropractic.com",  # generic, wrong service (radiological → chiropractic)
    "coffeyvillechiropractic.com",  # wrong practice (Advanced PT Coffeyville)
})

_HEALTH_KW = re.compile(
    r'\b(chiropractic|chiropractor|physical\s+therapy|physical\s+therapist|'
    r'therapist|therapy|rehab|rehabilitation|spine|spinal|wellness|chiro|'
    r'orthopedic|acupuncture|massage\s+therapy|PT\b|DC\b|DPT\b|'
    r'kinesiology|kinesiologist|adjustment|manipulation|musculoskeletal|'
    r'sports\s+medicine|sports\s+injury|back\s+pain|neck\s+pain)\b',
    re.I
)

_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")


def _norm_domain(url: str) -> str:
    try:
        return re.sub(r"^www\.", "", urlparse(url.strip()).netloc.lower())
    except Exception:
        return ""


def _domain_part(url: str) -> str:
    """Return the part before the TLD (e.g. 'pomeroychiro' from 'pomeroychiro.com')."""
    d = _norm_domain(url)
    parts = d.rsplit(".", 1)
    return parts[0] if parts else d


def _clean_name(name: str) -> str:
    out = re.sub(r",?\s*\b(llc|pllc|inc|pa|ltd|corp|dba)\b", "", name, flags=re.I)
    out = re.sub(r",?\s*\b(d\.?c\.?|dpt|pt|lpt|mspt|mpt|dc|md)\b", "", out, flags=re.I)
    return re.sub(r"\s+", " ", out).strip().strip(",").strip()


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


def _distinctive_name_words(name: str) -> list[str]:
    cleaned = _clean_name(name).lower()
    words = [w for w in re.split(r"\W+", cleaned) if len(w) >= 4 and w not in _GENERIC]
    return words


def _domain_contains_name_hint(url: str, name: str) -> bool:
    dp = _domain_part(url)
    words = _distinctive_name_words(name)
    return any(w in dp for w in words)


def _verify_content(url: str) -> bool:
    """GET homepage and check for health/chiro keywords in first 8KB."""
    try:
        r = requests.get(url, timeout=(6, 12), headers={"User-Agent": _UA},
                         allow_redirects=True, verify=False)
        if r.status_code >= 400:
            return False
        text = r.text[:8000]
        return bool(_HEALTH_KW.search(text))
    except Exception:
        return False


def should_remove(url: str, name: str) -> tuple[bool, str]:
    """Return (should_remove, reason)."""
    dp = _domain_part(url)
    d = _norm_domain(url)

    # Rule 1: domain part too short (2-5 chars → almost certainly wrong)
    if len(dp) <= 4:
        return True, f"domain part too short ({len(dp)} chars): {dp}"

    # Rule 2: known brand/unrelated domain
    if d in _BRAND_DOMAINS:
        return True, f"known false positive: {d}"

    # Rule 3: if domain DOES contain a distinctive name word, it's probably fine — skip content check
    if _domain_contains_name_hint(url, name):
        return False, "domain contains name hint — trusted"

    # Rule 4: content verification for everything else
    time.sleep(0.2)
    if not _verify_content(url):
        return True, f"no health keywords on homepage: {d}"

    return False, "passed content verification"


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


def main():
    all_csvs = sorted(DATA_DIR.glob("providers-*-ks.csv"))

    # Collect all websites from all CSVs, deduplicate for content-check caching
    url_verdict: dict[str, tuple[bool, str]] = {}

    total_removed = 0
    total_records = 0

    for csv_path in all_csvs:
        rows = list(csv.DictReader(csv_path.open()))
        if not rows:
            continue

        changed = False
        for r in rows:
            web = r.get("website", "").strip()
            if not web:
                continue
            total_records += 1

            if web not in url_verdict:
                remove, reason = should_remove(web, r.get("provider_name", ""))
                url_verdict[web] = (remove, reason)
                if remove:
                    print(f"  REMOVE: {r['provider_name'][:50]} → {web} ({reason})", flush=True)
            else:
                remove, _ = url_verdict[web]

            if remove:
                r["website"] = ""
                changed = True
                total_removed += 1

        if changed:
            with csv_path.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
                w.writeheader()
                w.writerows(rows)

    print(f"\nRemoved {total_removed} false-positive websites out of {total_records} checked")

    # Re-run global sharing (some records lost websites; re-propagate from valid ones)
    print("\nRe-running global address/phone sharing...", flush=True)
    all_records: list[dict] = []
    file_slices = []
    for p in all_csvs:
        rows = list(csv.DictReader(p.open()))
        file_slices.append((p, rows))
        all_records.extend(rows)

    shared = share_address_phone(all_records)
    print(f"  Re-shared: +{shared}", flush=True)

    for p, rows in file_slices:
        with p.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)

    # Final count
    total = sum(len(list(csv.DictReader(p.open()))) for p in all_csvs)
    with_web = sum(
        sum(1 for r in csv.DictReader(p.open()) if r.get("website", "").strip())
        for p in all_csvs
    )
    print(f"\nFinal: {with_web}/{total} ({100*with_web/total:.1f}%) providers have website")


if __name__ == "__main__":
    main()
