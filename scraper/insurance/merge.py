"""Merge captive/independent agent lists with the raw Foursquare sweep.

Marks Foursquare records as duplicates of already-verified captive/independent
agents (by phone match, or fuzzy name+city match), backfilling missing
website/phone on the known record from the Foursquare hit when useful.
Everything left over is written out for a follow-up classification pass
(auto-insurance offered? captive or independent? which company?).
"""
import json
import re
from rapidfuzz import fuzz

OUT = "scraper/insurance/output"


def load(name):
    with open(f"{OUT}/{name}") as f:
        return json.load(f)


def digits(s):
    return re.sub(r"\D", "", s or "")


def norm_name(s):
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9 ]", "", s)
    s = re.sub(r"\b(insurance|agency|agencies|group|ins|inc|llc|co|the)\b", "", s)
    return re.sub(r"\s+", " ", s).strip()


known = load("captive_batch1.json") + load("captive_batch2.json") + load("independent_agents.json")

known_by_phone = {}
for r in known:
    p = digits(r.get("phone_number"))
    if p:
        known_by_phone.setdefault(p, []).append(r)

fsq = load("api_agents_raw.json")

matched = 0
unmatched = []
for f in fsq:
    fp = digits(f.get("phone"))
    fn = norm_name(f.get("name"))
    fc = (f.get("city") or "").strip().lower()

    hit = None
    if fp and fp in known_by_phone:
        hit = known_by_phone[fp][0]
    else:
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

print(f"known={len(known)} fsq_total={len(fsq)} matched_dupes={matched} net_new_for_review={len(unmatched)}")

with open(f"{OUT}/known_merged.json", "w") as fh:
    json.dump(known, fh, indent=2)

with open(f"{OUT}/foursquare_unmatched.json", "w") as fh:
    json.dump(unmatched, fh, indent=2)
