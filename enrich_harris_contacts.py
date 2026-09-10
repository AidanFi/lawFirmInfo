#!/usr/bin/env python3
"""
Concurrent website/phone/email enrichment for a county CSV.

Two passes:
1. Rows that already have a website but are missing phone/email — fetch
   the known site directly and extract.
2. Rows with no website — try domain-guessing (law_domain_guess.py,
   validated against firm name + city/zip/phone so it can't false-positive
   on a same-named firm elsewhere), and on success extract phone/email
   from that same page fetch (no extra request).

Saves progress to disk periodically so an interrupted run doesn't lose work.

Usage: python3 enrich_harris_contacts.py <slug> [--limit N] [--min-lawyers N]
"""
import argparse
import csv
import re
import sys
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

warnings.filterwarnings("ignore", message="Unverified HTTPS request")

from law_domain_guess import domain_candidates, validate_candidate, HEADERS
from contact_extract import extract_phone, extract_email

DATA_DIR = Path("app/county-data")
FIELDNAMES = [
    "law_firm_name", "website", "google_business_profile", "legal_directory_listing",
    "city", "state", "county", "phone_number", "email", "practice_area",
    "street_address", "zip_code", "msa", "priority", "number_of_lawyers",
    "date_pulled", "source",
]


def _fetch(url: str, timeout: int = 6):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, verify=False, allow_redirects=True)
        if r.status_code < 400:
            return r
    except Exception:
        pass
    return None


def _fetch_office_subpage_text(base_url: str, home_text: str, city: str) -> str:
    """Many firm homepages (especially multi-office national firms) don't
    show a phone number at all — it lives on a dedicated offices/locations
    or contact page. Follow the first such link (preferring one whose
    anchor mentions the target city, e.g. "Houston") and return its text,
    so the caller can pool it with the homepage for phone/email extraction."""
    candidates = []
    for m in re.finditer(r'href="([^"]+)"[^>]{0,80}>([^<]{0,60})', home_text, re.IGNORECASE):
        href, anchor_text = m.group(1), m.group(2)
        combined = (href + " " + anchor_text).lower()
        if city and city.lower() in combined:
            candidates.append((0, href))
        elif re.search(r'contact|location|office', combined):
            candidates.append((1, href))
    candidates.sort(key=lambda c: c[0])
    for _, href in candidates[:3]:
        url = urljoin(base_url, href)
        if urlparse(url).netloc != urlparse(base_url).netloc:
            continue
        r = _fetch(url, timeout=5)
        if r:
            return r.text
    return ""


def enrich_known_website(row: dict) -> dict:
    """Row already has a website — fetch it and fill in missing phone/email."""
    updates = {}
    if row["phone_number"] and row["email"]:
        return updates
    r = _fetch(row["website"])
    if not r:
        return updates
    text = r.text
    city = row.get("city", "")
    needs_phone = not row["phone_number"] and not extract_phone(text, prefer_context=city)
    needs_email = not row["email"] and not extract_email(text)
    if needs_phone or needs_email:
        text += "\n" + _fetch_office_subpage_text(r.url, text, city)
    domain = urlparse(row["website"]).netloc.lower().lstrip("www.")
    if not row["phone_number"]:
        phone = extract_phone(text, prefer_context=city)
        if phone:
            updates["phone_number"] = phone
    if not row["email"]:
        email = extract_email(text, firm_domain=domain)
        if email:
            updates["email"] = email
    return updates


def enrich_no_website(row: dict) -> dict:
    """Row has no website — try to find one via domain guessing, then
    extract phone/email from the same fetch."""
    name = row["law_firm_name"]
    city = row.get("city", "")
    state = row.get("state", "TX")
    zip_code = row.get("zip_code", "")
    phone = row.get("phone_number", "")

    for url in domain_candidates(name):
        r = _fetch(url)
        if not r:
            continue
        if not validate_candidate(url, name, city, state, zip_code, phone):
            continue
        # Store the resolved final URL, not the guessed candidate — some
        # firms own a vanity/redirect domain that correctly forwards to
        # their real canonical site (verified: kirklandellislaw.com ->
        # kirkland.com, kingspaldinglaw.com -> kslaw.com), and the
        # canonical form is what a visitor should be given.
        final_url = r.url or url
        updates = {"website": final_url}
        domain = urlparse(final_url).netloc.lower().lstrip("www.")
        text = r.text
        needs_phone = not row["phone_number"] and not extract_phone(text, prefer_context=city)
        needs_email = not row["email"] and not extract_email(text)
        if needs_phone or needs_email:
            text += "\n" + _fetch_office_subpage_text(final_url, text, city)
        if not row["phone_number"]:
            found_phone = extract_phone(text, prefer_context=city)
            if found_phone:
                updates["phone_number"] = found_phone
        if not row["email"]:
            found_email = extract_email(text, firm_domain=domain)
            if found_email:
                updates["email"] = found_email
        return updates
    return {}


def process_row(idx_row):
    idx, row = idx_row
    try:
        if row["website"]:
            updates = enrich_known_website(row)
        else:
            updates = enrich_no_website(row)
    except Exception:
        updates = {}
    return idx, updates


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("slug")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--min-lawyers", type=int, default=0)
    ap.add_argument("--workers", type=int, default=20)
    ap.add_argument("--only", choices=["known", "unknown", "both"], default="both")
    args = ap.parse_args()

    csv_path = DATA_DIR / f"{args.slug}.csv"
    rows = list(csv.DictReader(open(csv_path, encoding="utf-8")))
    print(f"Loaded {len(rows)} rows")

    targets = []
    for i, r in enumerate(rows):
        if int(r.get("number_of_lawyers") or 0) < args.min_lawyers:
            continue
        has_web = bool(r["website"])
        if args.only == "known" and not has_web:
            continue
        if args.only == "unknown" and has_web:
            continue
        if has_web and r["phone_number"] and r["email"]:
            continue
        targets.append(i)

    # Prioritize bigger firms first (more value per successful hit).
    targets.sort(key=lambda i: -int(rows[i].get("number_of_lawyers") or 0))
    if args.limit:
        targets = targets[: args.limit]

    print(f"Processing {len(targets)} target rows with {args.workers} workers")

    website_found = 0
    phone_found = 0
    email_found = 0
    done = 0
    save_every = 200

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(process_row, (i, rows[i])): i for i in targets}
        for fut in as_completed(futures):
            idx, updates = fut.result()
            if updates:
                if updates.get("website") and not rows[idx]["website"]:
                    website_found += 1
                if updates.get("phone_number") and not rows[idx]["phone_number"]:
                    phone_found += 1
                if updates.get("email") and not rows[idx]["email"]:
                    email_found += 1
                rows[idx].update(updates)
            done += 1
            if done % 100 == 0:
                print(f"  progress: {done}/{len(targets)} | +website {website_found} | +phone {phone_found} | +email {email_found}")
            if done % save_every == 0:
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    w = csv.DictWriter(f, fieldnames=FIELDNAMES)
                    w.writeheader()
                    w.writerows(rows)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(rows)

    print(f"\nDone. +website {website_found}, +phone {phone_found}, +email {email_found}")


if __name__ == "__main__":
    main()
