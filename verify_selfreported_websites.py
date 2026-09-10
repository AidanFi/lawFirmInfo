#!/usr/bin/env python3
"""
Verify self-reported (merge_detail_data.py) AND Justia-backfilled
(merge_justia_data.py) websites actually relate to the firm/attorney
they're attached to, and extract phone/email from the same fetch.

Self-reported bar-profile data is a strong signal, but verified NOT
infallible: some profiles are stale (a "website" field still pointing to
a job the attorney left — e.g. an in-house counsel whose profile still
listed their former BigLaw firm's site) or outright data-entry errors (a
firm name or an email address typed into the URL field). Justia's
directory data has the SAME failure mode from a different cause: Justia
matches by individual attorney name, and for a multi-lawyer firm row,
one attorney's stale/wrong bio-page link (e.g. their old firm from
before a lateral move) silently becomes the whole FIRM's website —
verified in the wild on Dallas County: "Bell Nunnally & Martin LLP"
picked up "gibsondunn.com/Lawyers/jguild" this way. Both sources get the
same light-touch check here — does the page mention a distinctive token
from the firm name plus an actual law-practice term — and the website is
cleared if not, rather than trusted blindly. Unlike domain-guessing, this
does NOT require proximity to a location signal (both sources are
already a much stronger prior than a guessed domain), just basic
relatedness.
"""
import argparse
import csv
import re
import sys
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

import requests

warnings.filterwarnings("ignore", message="Unverified HTTPS request")

sys.path.insert(0, ".")
from law_domain_guess import _tokens, HEADERS
from contact_extract import extract_phone, extract_email

DATA_DIR = Path("app/county-data")
FIELDNAMES = [
    "law_firm_name", "website", "google_business_profile", "legal_directory_listing",
    "city", "state", "county", "phone_number", "email", "practice_area",
    "street_address", "zip_code", "msa", "priority", "number_of_lawyers",
    "date_pulled", "source",
]


def _fetch(url: str, timeout: int = 8):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, verify=False, allow_redirects=True)
        if r.status_code < 400:
            return r
    except Exception:
        pass
    return None


def _looks_like_email_or_junk(url: str) -> bool:
    if "@" in url:
        return True
    parsed = urlparse(url)
    if not parsed.netloc or " " in parsed.netloc:
        return True
    if "." not in parsed.netloc:
        return True
    return False


def verify_row(row: dict) -> dict:
    url = row["website"]
    updates = {}
    if _looks_like_email_or_junk(url):
        # A bare email typed into the website field — recover it as the
        # email if we don't have one yet, and drop the bogus "website".
        m = re.search(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', url)
        if m and not row["email"]:
            updates["email"] = m.group(0).lower()
        updates["website"] = ""
        return updates

    r = _fetch(url)
    if not r:
        updates["website"] = ""
        return updates

    text = r.text.lower()
    # Require MULTIPLE distinctive tokens to match for a 3+ word name, not
    # just any one — a single common given name or short surname can
    # coincidentally appear somewhere on an unrelated (often large) site.
    # Verified in the wild: "Jose Luis Trevino Alanis" wrongly passed
    # because "Luis" alone appeared elsewhere on a firm's 487KB page with
    # zero connection to him.
    name_words = [w for w in _tokens(row["law_firm_name"]) if len(w) >= 4]
    if name_words:
        hits = sum(1 for w in name_words if w in text)
        required = 2 if len(name_words) >= 3 else len(name_words)
        name_related = hits >= required
    else:
        name_related = True

    # Require an actual law-practice signal too, not just a name-token
    # match — a name built from generic English words (e.g. "Valid
    # Management, LLC") can coincidentally match boilerplate on any
    # unrelated corporate site. Verified in the wild: an attorney's stale
    # self-reported website pointed to his former employer, a logistics
    # company whose site happened to contain the word "management".
    has_law_term = bool(re.search(
        r'\battorney|\blawyer|law firm|law office|legal services|counselor at law',
        text,
    ))

    if not (name_related and has_law_term):
        updates["website"] = ""
        return updates

    updates["website"] = r.url  # canonical resolved URL
    domain = urlparse(r.url).netloc.lower().lstrip("www.")
    if not row["phone_number"]:
        phone = extract_phone(text, prefer_context=row.get("city", ""))
        if phone:
            updates["phone_number"] = phone
    if not row["email"]:
        email = extract_email(text, firm_domain=domain)
        if email:
            updates["email"] = email
    return updates


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("slug")
    ap.add_argument("--workers", type=int, default=25)
    args = ap.parse_args()

    csv_path = DATA_DIR / f"{args.slug}.csv"
    rows = list(csv.DictReader(open(csv_path, encoding="utf-8")))

    targets = [
        i for i, r in enumerate(rows)
        if r["website"] and ("self-reported" in r["source"] or "Justia" in r["source"])
    ]
    print(f"Verifying {len(targets)} self-reported/Justia websites")

    cleared = 0
    kept = 0
    phone_added = 0
    email_added = 0
    done = 0

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(verify_row, rows[i]): i for i in targets}
        for fut in as_completed(futures):
            idx = futures[fut]
            updates = fut.result()
            if "website" in updates:
                if updates["website"]:
                    rows[idx]["website"] = updates["website"]
                    kept += 1
                else:
                    rows[idx]["website"] = ""
                    cleared += 1
            if updates.get("email") and not rows[idx]["email"]:
                rows[idx]["email"] = updates["email"]
                email_added += 1
            if updates.get("phone_number") and not rows[idx]["phone_number"]:
                rows[idx]["phone_number"] = updates["phone_number"]
                phone_added += 1
            done += 1
            if done % 300 == 0:
                print(f"  progress: {done}/{len(targets)} | kept {kept} | cleared {cleared}")

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(rows)

    print(f"\nDone. Kept {kept}, cleared (unrelated/unreachable) {cleared}")
    print(f"Bonus: +phone {phone_added}, +email {email_added}")


if __name__ == "__main__":
    main()
