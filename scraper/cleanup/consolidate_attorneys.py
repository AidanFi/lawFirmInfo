#!/usr/bin/env python3
"""Consolidate person-named "firms" into their employer firms.

Many entries in our data are individual attorneys (scraped from KS Courts)
that are actually employees of firms already in our dataset. This pass
merges them as attorney records on the existing firms, which:
  - reduces the denominator of missing-website firms honestly
  - preserves the attorney's contact info on the parent firm record

Matching rules (conservative to avoid bad merges):
  1. Phone number match (strong signal) — merge into matching firm
  2. OR: Exact street+city address match with a firm in the same city

Person is NOT merged if:
  - They have their own website (likely solo practitioner)
  - They have no address AND no phone (can't verify match)
  - Multiple candidate firms match (ambiguous)

Usage:
    python -m scraper.cleanup.consolidate_attorneys --dry-run
    python -m scraper.cleanup.consolidate_attorneys
"""
import argparse
import json
import re
import shutil
from collections import defaultdict
from datetime import datetime

INPUT_PATH = "app/firms_data.js"
BACKUP_PATH = f"/tmp/firms_data_consolidate_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.js"

FIRM_TOKENS = (
    " llc", " llp", " l.l.p", " l.l.c", " pa", " p.a.",
    " law", " firm", " office", " associates", " group",
    " & ", " chartered", " attorneys", " partners", " pllc",
    " lc", " l.c.",
)


def _load_firms():
    with open(INPUT_PATH) as f:
        content = f.read()
    return json.loads(content[len("const FIRMS_DATA = "):-1])


def _save_firms(data):
    with open(INPUT_PATH, "w") as f:
        f.write("const FIRMS_DATA = ")
        json.dump(data, f, indent=2)
        f.write(";")


def _is_person_like(name: str) -> bool:
    if not name:
        return False
    lower = name.lower()
    if any(t in lower for t in FIRM_TOKENS):
        return False
    if any(c.isdigit() for c in name):
        return False
    words = name.replace(",", " ").split()
    if len(words) < 2 or len(words) > 4:
        return False
    cap_words = sum(1 for w in words if w and w[0].isupper())
    return cap_words == len(words)


def _norm_phone(phone):
    if not phone:
        return ""
    return re.sub(r"\D", "", str(phone))


def _norm_street(street):
    if not street:
        return ""
    s = street.lower().strip()
    # Normalize common suffixes
    s = re.sub(r"\b(street|st)\b\.?", "st", s)
    s = re.sub(r"\b(avenue|ave)\b\.?", "ave", s)
    s = re.sub(r"\b(drive|dr)\b\.?", "dr", s)
    s = re.sub(r"\b(road|rd)\b\.?", "rd", s)
    s = re.sub(r"\b(boulevard|blvd)\b\.?", "blvd", s)
    s = re.sub(r"\b(suite|ste)\b\.?\s*\d+", "", s)
    s = re.sub(r"#\s*\d+", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    data = _load_firms()
    firms = data["firms"]
    before = len(firms)

    # Partition
    firms_real = []
    persons = []
    for f in firms:
        (persons if _is_person_like(f.get("name", "")) else firms_real).append(f)

    # Index firms_real by phone and by (street, city)
    by_phone = defaultdict(list)
    by_addr = defaultdict(list)
    for f in firms_real:
        p = _norm_phone(f.get("phone"))
        if p and len(p) >= 10:
            by_phone[p[-10:]].append(f)
        addr = f.get("address") or {}
        street = _norm_street(addr.get("street"))
        city = (addr.get("city") or "").lower()
        if street and city:
            by_addr[(street, city)].append(f)

    merged = 0
    ambiguous = 0
    unmatched = 0
    skipped_has_website = 0
    skipped_no_contact = 0

    remaining_persons = []

    for p in persons:
        if p.get("website"):
            skipped_has_website += 1
            remaining_persons.append(p)
            continue

        phone = _norm_phone(p.get("phone"))
        addr = p.get("address") or {}
        street = _norm_street(addr.get("street"))
        city = (addr.get("city") or "").lower()

        candidates = []
        if phone and len(phone) >= 10:
            candidates.extend(by_phone.get(phone[-10:], []))
        if street and city:
            candidates.extend(by_addr.get((street, city), []))

        # Dedupe
        seen_ids = set()
        uniq = []
        for c in candidates:
            cid = id(c)
            if cid not in seen_ids:
                seen_ids.add(cid)
                uniq.append(c)

        if not uniq:
            if not phone and not (street and city):
                skipped_no_contact += 1
            else:
                unmatched += 1
            remaining_persons.append(p)
            continue

        if len(uniq) > 1:
            # If all candidates are the same firm (same phone, same addr), merge
            # Otherwise ambiguous
            names_set = {c.get("name") for c in uniq}
            if len(names_set) > 1:
                ambiguous += 1
                remaining_persons.append(p)
                continue

        target = uniq[0]
        attorneys = target.setdefault("attorneys", [])
        # Avoid dup: skip if person's name already listed
        pname = p.get("name", "").strip()
        if pname and pname.lower() not in (a.get("name", "").lower() for a in attorneys):
            rec = {"name": pname}
            if p.get("phone"):
                rec["phone"] = p["phone"]
            if p.get("email"):
                rec["email"] = p["email"]
            attorneys.append(rec)
        if "ks_courts" in (p.get("sources") or []):
            if "ks_courts" not in (target.get("sources") or []):
                target.setdefault("sources", []).append("ks_courts")
        merged += 1

    firms_new = firms_real + remaining_persons

    print("=" * 60)
    print("  Attorney Consolidation")
    print("=" * 60)
    print(f"  Before:                   {before} firms")
    print(f"  Real firms:               {len(firms_real)}")
    print(f"  Person-like:              {len(persons)}")
    print(f"    -> merged:              {merged}")
    print(f"    -> unmatched:           {unmatched}")
    print(f"    -> ambiguous:           {ambiguous}")
    print(f"    -> skipped (website):   {skipped_has_website}")
    print(f"    -> skipped (no contact):{skipped_no_contact}")
    print(f"  After:                    {len(firms_new)} firms")
    print("=" * 60)

    if args.dry_run:
        print("[DRY RUN] No changes saved")
        return

    shutil.copy(INPUT_PATH, BACKUP_PATH)
    print(f"Backed up to {BACKUP_PATH}")

    data["firms"] = firms_new
    data["meta"]["lastConsolidate"] = datetime.now().isoformat()
    _save_firms(data)
    print(f"Saved {len(firms_new)} firms to {INPUT_PATH}")


if __name__ == "__main__":
    main()
