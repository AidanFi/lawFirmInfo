#!/usr/bin/env python3
"""
Backfill website (and phone, as a secondary check) from the Justia
discovery cache into existing CSV rows — never adds new rows (the State
Bar registry is already the authoritative, exhaustive attorney list).

Matching is intentionally NOT fuzzy for personal names: this project
already found that fuzzy/ratio-based name matching produces false merges
between different real people who share most tokens (e.g. "Christina
Marie Lyons" vs "Christina Marie Clayton"). Instead this requires an
EXACT token-set match (same set of name words, order-independent, after
stripping punctuation/suffixes) plus the same city.
"""
import csv
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse, urlunparse

DATA_DIR = Path("app/county-data")
CACHE_DIR = Path("data/county")

FIELDNAMES = [
    "law_firm_name", "website", "google_business_profile", "legal_directory_listing",
    "city", "state", "county", "phone_number", "email", "practice_area",
    "street_address", "zip_code", "msa", "priority", "number_of_lawyers",
    "date_pulled", "source",
]

_SUFFIX_RE = re.compile(r'\b(jr|sr|ii|iii|iv|esq)\b\.?', re.IGNORECASE)


def name_tokens(name: str) -> frozenset:
    n = _SUFFIX_RE.sub("", name.lower())
    n = re.sub(r"[^a-z\s]", " ", n)
    return frozenset(t for t in n.split() if len(t) >= 2)


def clean_url(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse(parsed._replace(query="", fragment=""))


def main(slug: str):
    csv_path = DATA_DIR / f"{slug}.csv"
    justia_path = CACHE_DIR / f"{slug}_justia_cache.json"
    raw_cache_path = CACHE_DIR / f"{slug}_statebar_cache.json"
    sidecar_path = CACHE_DIR / f"{slug}_row_contact_ids.json"

    rows = list(csv.DictReader(open(csv_path, encoding="utf-8")))
    justia = json.loads(justia_path.read_text())
    raw_cache = {e["contact_id"]: e for e in json.loads(raw_cache_path.read_text()) if e.get("contact_id")}
    sidecar = json.loads(sidecar_path.read_text())

    # Build (token-set, city) -> justia entry, for entries that have a website.
    justia_by_key = defaultdict(list)
    for j in justia:
        if not j.get("website"):
            continue
        justia_by_key[(name_tokens(j["name"]), j["city"].lower())].append(j)

    website_added = 0
    phone_added = 0

    for r in rows:
        if r["website"]:
            continue
        candidates = []

        # Solo row: law_firm_name IS the attorney's own name.
        candidates.append((name_tokens(r["law_firm_name"]), r["city"].lower()))

        # Grouped firm row: also check each underlying member's own name.
        key = f"{r['law_firm_name']}|||{r['city']}"
        for cid in sidecar.get(key, []):
            member = raw_cache.get(cid)
            if member and member.get("name"):
                candidates.append((name_tokens(member["name"]), r["city"].lower()))

        matched = None
        for cand_tokens, cand_city in candidates:
            if not cand_tokens:
                continue
            hits = justia_by_key.get((cand_tokens, cand_city))
            if hits:
                matched = hits[0]
                break

        if matched:
            r["website"] = clean_url(matched["website"])
            website_added += 1
            if not r["phone_number"] and matched.get("phone"):
                digits = re.sub(r"\D", "", matched["phone"])
                if len(digits) == 10:
                    r["phone_number"] = f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
                    phone_added += 1
            src = r["source"]
            if "Justia" not in src:
                r["source"] = f"{src}; Justia"

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(rows)

    print(f"Websites added from Justia (exact name+city match): {website_added}")
    print(f"Phones added: {phone_added}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 merge_justia_data.py <slug>")
        sys.exit(1)
    main(sys.argv[1])
