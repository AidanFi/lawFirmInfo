#!/usr/bin/env python3
"""
Match the Avvo discovery cache (avvo_tx_discover.py) against an existing
county CSV and backfill website + phone on matching rows.

Matching is EXACT token-set name + city (same conservative policy as
merge_justia_data.py — this project already found that fuzzy/ratio-based
matching on personal names produces false merges between different real
people who share most tokens).

Avvo's list-page JSON-LD does NOT include a website — only the detail
page does (a JSON-LD "LocalBusiness" block's "sameAs" array, whose first
non-social-media entry is the firm's own site). So this script only
fetches a DETAIL page for a row that (a) matched an existing CSV row and
(b) that row currently has no website — bounding the expensive per-
attorney fetch to only the rows we can actually use it for, instead of
fetching every discovered attorney's detail page.
"""
import argparse
import csv
import json
import re
import sys
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

from curl_cffi import requests as creq
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore", message="Unverified HTTPS request")

sys.path.insert(0, ".")
from law_domain_guess import _tokens
from contact_extract import extract_phone, extract_email

DATA_DIR = Path("app/county-data")
CACHE_DIR = Path("data/county")
FIELDNAMES = [
    "law_firm_name", "website", "google_business_profile", "legal_directory_listing",
    "city", "state", "county", "phone_number", "email", "practice_area",
    "street_address", "zip_code", "msa", "priority", "number_of_lawyers",
    "date_pulled", "source",
]

_SUFFIX_RE = re.compile(r'\b(jr|sr|ii|iii|iv|esq)\b\.?', re.IGNORECASE)
_SOCIAL_RE = re.compile(
    r'facebook\.com|twitter\.com|x\.com|linkedin\.com|instagram\.com|'
    r'youtube\.com|avvo\.com|tiktok\.com|yelp\.com',
    re.IGNORECASE,
)


def name_tokens(name: str) -> frozenset:
    n = _SUFFIX_RE.sub("", name.lower())
    n = re.sub(r"[^a-z\s]", " ", n)
    return frozenset(t for t in n.split() if len(t) >= 2)


def fetch_detail_website(session, avvo_url: str) -> str:
    """Fetch an Avvo attorney detail page and return the first non-social
    URL in the LocalBusiness JSON-LD 'sameAs' array, or '' if none.

    Avvo's detail pages are much more aggressively bot-protected than its
    list pages — even at moderate concurrency, requests get served a
    Cloudflare "Just a moment..." JS-challenge page (sometimes as a 200,
    not just 429), which silently looks like "no website found" unless
    explicitly detected. Retry with real backoff on either signal."""
    if not avvo_url:
        return ""
    for attempt in range(4):
        try:
            r = session.get(avvo_url, timeout=15)
            if r.status_code == 429 or "Just a moment" in r.text[:2000]:
                time.sleep(30 * (attempt + 1))
                continue
            if r.status_code != 200:
                return ""
            soup = BeautifulSoup(r.text, "lxml")
            for block in soup.find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(block.string)
                except Exception:
                    continue
                if data.get("@type") != "LocalBusiness":
                    continue
                for url in data.get("sameAs") or []:
                    if not _SOCIAL_RE.search(url):
                        parsed = urlparse(url)
                        if parsed.scheme and parsed.netloc:
                            return url
            return ""
        except Exception:
            time.sleep(2)
    return ""


def main(slug: str, workers: int):
    csv_path = DATA_DIR / f"{slug}.csv"
    avvo_path = CACHE_DIR / f"{slug}_avvo_cache.json"

    rows = list(csv.DictReader(open(csv_path, encoding="utf-8")))
    avvo = json.loads(avvo_path.read_text())
    print(f"Base CSV: {len(rows)} rows. Avvo cache: {len(avvo)} attorneys.")

    # Build (name_tokens, city) -> avvo entries index.
    avvo_by_key = {}
    for a in avvo:
        key = (name_tokens(a["name"]), a["city"].lower().strip())
        avvo_by_key.setdefault(key, []).append(a)

    phone_added = 0
    matched_no_website = []  # (row, avvo_entry) needing a detail-page fetch

    for r in rows:
        # Solo row: law_firm_name IS the attorney's own name.
        candidates = [(name_tokens(r["law_firm_name"]), r["city"].lower().strip())]
        matched = None
        for key in candidates:
            hits = avvo_by_key.get(key)
            if hits:
                matched = hits[0]
                break
        if not matched:
            continue

        if not r["phone_number"] and matched.get("phone"):
            digits = re.sub(r"\D", "", matched["phone"])
            if len(digits) == 10:
                r["phone_number"] = f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
                phone_added += 1

        if not r["website"] and matched.get("avvo_url"):
            matched_no_website.append((r, matched))

    print(f"Matched {len(matched_no_website)} no-website rows to an Avvo profile — fetching detail pages...")

    session_local = {"s": None}

    def get_session():
        if session_local["s"] is None:
            session_local["s"] = creq.Session(impersonate="chrome120")
        return session_local["s"]

    website_added = 0
    email_added = 0
    done = 0

    def worker(row, avvo_entry):
        session = creq.Session(impersonate="chrome120")
        time.sleep(0.5)
        url = fetch_detail_website(session, avvo_entry["avvo_url"])
        return row, url

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(worker, r, a): r for r, a in matched_no_website}
        for fut in as_completed(futures):
            row, url = fut.result()
            if url:
                row["website"] = url
                src = row["source"]
                if "Avvo" not in src:
                    row["source"] = f"{src}; Avvo" if src else "Avvo"
                website_added += 1
            done += 1
            if done % 100 == 0:
                print(f"  progress: {done}/{len(matched_no_website)} | websites found: {website_added}")

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(rows)

    print(f"\nDone. Websites added from Avvo: {website_added}. Phones added: {phone_added}.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("slug")
    ap.add_argument("--workers", type=int, default=4)
    args = ap.parse_args()
    main(args.slug, args.workers)
