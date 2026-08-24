"""Generalized version of merge.py: merges captive/independent agent lists
with raw Foursquare + Google Places sweeps for any county subdirectory
under scraper/insurance/output/<county>/.

Marks broad-sweep records as duplicates of already-verified captive/
independent agents (by phone match, or fuzzy name+city match). Everything
left over is written out for a follow-up classification pass.

Usage: python3 scraper/insurance/merge_county.py <county_dir>
"""
import json
import re
import sys
from rapidfuzz import fuzz


TOLL_FREE_PREFIXES = ("800", "888", "877", "866", "855", "844", "833")


def digits(s):
    return re.sub(r"\D", "", s or "")


def is_toll_free(phone_digits: str) -> bool:
    # A shared national toll-free number does NOT imply the same physical
    # office - multi-branch orgs route several distinct locations through
    # one member-service line. Only treat it as a match if city also matches.
    d = phone_digits[1:] if len(phone_digits) == 11 and phone_digits.startswith("1") else phone_digits
    return d[:3] in TOLL_FREE_PREFIXES


def norm_name(s):
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9 ]", "", s)
    s = re.sub(r"\b(insurance|agency|agencies|group|ins|inc|llc|co|the|associates|services)\b", "", s)
    return re.sub(r"\s+", " ", s).strip()


def load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return []


def main():
    county_dir = sys.argv[1]
    out = f"scraper/insurance/output/{county_dir}"

    known = (load(f"{out}/captive_batch1.json") + load(f"{out}/captive_batch2.json")
             + load(f"{out}/independent_agents.json"))

    known_by_phone = {}
    for r in known:
        p = digits(r.get("phone_number"))
        if p:
            known_by_phone.setdefault(p, []).append(r)

    broad_sweep = load(f"{out}/foursquare_raw.json") + load(f"{out}/google_places_raw.json")
    # google_places_raw uses {name, phone, formatted_address, city_hint, website}
    # foursquare_raw uses {name, phone, street_address, city, zip, website}
    # normalize into a common shape for downstream matching
    normalized = []
    for r in broad_sweep:
        if "formatted_address" in r:
            normalized.append({
                "name": r.get("name", ""),
                "phone": r.get("phone", ""),
                "street_address": r.get("formatted_address", ""),
                "city": r.get("city_hint", ""),
                "zip": "",
                "website": r.get("website", ""),
                "source_url": r.get("place_id", ""),
                "source": r.get("source", "Google Places"),
            })
        else:
            normalized.append(r)

    matched = 0
    unmatched = []
    for f in normalized:
        fp = digits(f.get("phone"))
        fn = norm_name(f.get("name"))
        fc = (f.get("city") or "").strip().lower()

        hit = None
        if fp and fp in known_by_phone and not is_toll_free(fp):
            hit = known_by_phone[fp][0]
        elif fp and fp in known_by_phone:
            hit = next((r for r in known_by_phone[fp]
                        if (r.get("city") or "").strip().lower() == fc), None)
        if hit is None:
            for r in known:
                if (r.get("city") or "").strip().lower() != fc:
                    continue
                if fuzz.token_sort_ratio(fn, norm_name(r.get("agency_name") or r.get("agent_name"))) >= 88:
                    hit = r
                    break

        if hit:
            matched += 1
            if not hit.get("website") and f.get("website"):
                hit["website"] = f["website"]
            if not hit.get("phone_number") and f.get("phone"):
                hit["phone_number"] = f["phone"]
        else:
            unmatched.append(f)

    # dedupe within the unmatched broad-sweep pool itself (Foursquare + Places
    # both hit the same business independently)
    deduped_unmatched = []
    seen_phones = set()
    for f in unmatched:
        p = digits(f.get("phone"))
        if p and p in seen_phones:
            continue
        fn = norm_name(f.get("name"))
        fc = (f.get("city") or "").strip().lower()
        is_dupe = False
        for kept in deduped_unmatched:
            if (kept.get("city") or "").strip().lower() != fc:
                continue
            if fuzz.token_sort_ratio(fn, norm_name(kept.get("name"))) >= 90:
                is_dupe = True
                break
        if is_dupe:
            continue
        deduped_unmatched.append(f)
        if p:
            seen_phones.add(p)

    print(f"known={len(known)} broad_sweep_total={len(normalized)} matched_dupes={matched} "
          f"unmatched_raw={len(unmatched)} net_new_for_review={len(deduped_unmatched)}")

    with open(f"{out}/known_merged.json", "w") as fh:
        json.dump(known, fh, indent=2)

    with open(f"{out}/broad_sweep_unmatched.json", "w") as fh:
        json.dump(deduped_unmatched, fh, indent=2)


if __name__ == "__main__":
    main()
