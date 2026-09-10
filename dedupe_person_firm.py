#!/usr/bin/env python3
"""
Merge a bare personal-name row into that same person's firm-branded row
when both share the exact same address+phone — the "Abel Izaguirre" row
and "The Izaguirre Law Firm" row are the same solo attorney, one entry
just came from the bar registry (blank company) and the other from a
different source (Foursquare/Justia) with the firm's actual brand name.

Safeguard: NEVER merge two bare personal-name rows together just because
they share a surname and office — two different attorneys (e.g. a married
couple, or unrelated colleagues) can legitimately share both. Only merge
when the surname match is against a FIRM-BRANDED name (an explicit legal
suffix, "Law Firm"/"Law Office"/"& Associates" phrase, or multiple
comma/ampersand-joined surnames) — a real firm brand is what actually
represents "this business", so a matching solo entry is redundant with
it, whereas two personal names are each their own distinct identity.
"""
import csv
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, ".")
from statebar_to_csv import _tokens, LAW_FIRM_SUFFIX_RE

DATA_DIR = Path("app/county-data")
FIELDNAMES = [
    "law_firm_name", "website", "google_business_profile", "legal_directory_listing",
    "city", "state", "county", "phone_number", "email", "practice_area",
    "street_address", "zip_code", "msa", "priority", "number_of_lawyers",
    "date_pulled", "source",
]

# A comma is only reliable evidence of "multiple partner surnames joined"
# (e.g. "Stafford, Keyser, Bromberg") if what remains after stripping a
# trailing bare corporate/entity suffix STILL has a comma in it — a comma
# immediately before nothing but that suffix ("Hendricks Interests, LLC")
# is NOT a law firm and wrongly matched this way originally.
_TRAILING_ENTITY_SUFFIX_RE = re.compile(
    r',?\s*(llc|inc\.?|incorporated|corp\.?|corporation|ltd\.?|co\.?)\s*$',
    re.IGNORECASE,
)
_MULTI_SURNAME_RE = re.compile(r'&|\band\b', re.IGNORECASE)


def has_comma_joined_names(name: str) -> bool:
    stripped = _TRAILING_ENTITY_SUFFIX_RE.sub("", name).strip()
    return "," in stripped

# Surnames common enough that a single-token match is not reliable
# evidence of "same person" — verified in the wild: this heuristic
# initially proposed merging unrelated people who happened to share
# "Jones" or "Smith" and an office address. Require a corroborating
# first-name token too when the surname is this common.
_COMMON_SURNAMES = {
    "smith", "jones", "johnson", "williams", "brown", "davis", "garcia",
    "miller", "wilson", "anderson", "taylor", "thomas", "moore", "jackson",
    "martin", "lee", "thompson", "white", "harris", "clark", "lewis",
    "walker", "hall", "young", "king", "wright", "green", "baker", "york",
}

# Common GIVEN names are never reliable surname candidates — verified in
# the wild: "William David Rowlett" (middle name "David") falsely matched
# "Law Office of David A Breston" (an unrelated firm whose OWNER's first
# name happens to be David) purely because "David" isn't the first token
# and so wasn't excluded as "assumed given name."
_COMMON_GIVEN_NAMES = {
    "david", "john", "michael", "james", "robert", "william", "richard",
    "charles", "joseph", "thomas", "christopher", "daniel", "matthew",
    "anthony", "mark", "donald", "steven", "paul", "andrew", "joshua",
    "kenneth", "kevin", "brian", "george", "edward", "ronald", "timothy",
    "jason", "jeffrey", "ryan", "jacob", "gary", "nicholas", "eric",
    "stephen", "jonathan", "larry", "justin", "scott", "brandon",
    "benjamin", "samuel", "raymond", "patrick", "alexander", "jack",
    "dennis", "jerry", "mary", "patricia", "jennifer", "linda", "elizabeth",
    "barbara", "susan", "jessica", "sarah", "karen", "lisa", "nancy",
    "betty", "sandra", "margaret", "ashley", "kimberly", "emily", "donna",
    "michelle", "dorothy", "carol", "amanda", "melissa", "deborah",
    "stephanie", "rebecca", "laura", "sharon", "cynthia", "kathleen",
    "amy", "angela", "shirley", "anna", "brenda", "pamela", "emma",
    "nicole", "helen", "samantha", "katherine", "christine", "debra",
    "rachel", "carolyn", "janet", "catherine", "maria", "heather",
    "diane", "ruth", "julie", "alicia", "victoria", "douglas",
}

# A business descriptor in the name means it's a distinct company/service
# identity, not just "a person's name with no suffix" — verified in the
# wild: "Franklin York Mediations" (a wife's separate mediation practice)
# wrongly matched "Law Offices of Douglas Ray York, P.C." (her husband's
# separate law practice at the same shared office) purely on the shared
# family surname "York" — two real, distinct businesses, not a duplicate.
_BUSINESS_DESCRIPTOR_RE = re.compile(
    r'\bmediations?\b|\bconsulting\b|\badvisors?\b|\badvisory\b|\bsolutions\b|'
    r'\bholdings\b|\benterprises\b|\bresources\b|\binterests\b',
    re.IGNORECASE,
)


def is_firm_branded(name: str) -> bool:
    if LAW_FIRM_SUFFIX_RE.search(name):
        return True
    if re.search(
        r'\blaw firm\b|\blaw office\b|\blaw group\b|attorney(s)?\s+at\s+law|'
        r'counselor\s+at\s+law|attorney\s+&\s+counselor',
        name, re.IGNORECASE,
    ):
        return True
    if _MULTI_SURNAME_RE.search(name):
        return True
    if has_comma_joined_names(name):
        return True
    return False


def is_bare_personal_name(name: str) -> bool:
    """No firm-entity suffix, no comma/ampersand (so not 'X & Associates'
    or 'Smith, Jones'), no business-descriptor word, 2-4 words — looks
    like just a person's name, not a distinct company/service identity."""
    if is_firm_branded(name):
        return False
    if _BUSINESS_DESCRIPTOR_RE.search(name):
        return False
    words = name.split()
    return 2 <= len(words) <= 4


def surname_candidates(name: str) -> list[str]:
    """All plausible surname tokens, not just the last word — multi-part
    names (maiden name, hyphenated surname) can have the firm-relevant
    surname in the middle, e.g. "Ifi Achebe Iloani" -> firm "A. Bethea &
    Achebe P.C." matches on "Achebe", not the literal last token "Iloani".
    Excludes the first token (assumed given name) AND any token that's a
    common given name regardless of position (a middle name, or a first
    name in a "First Middle Last" pattern where position 0 isn't reliably
    "the" first name) — never a safe basis for an identity match alone."""
    toks = [t for t in _tokens(name) if len(t) >= 4]
    candidates = toks[1:] if len(toks) >= 2 else toks
    return [t for t in candidates if t not in _COMMON_GIVEN_NAMES]


def main(slug: str):
    csv_path = DATA_DIR / f"{slug}.csv"
    rows = list(csv.DictReader(open(csv_path, encoding="utf-8")))

    by_addr_phone = defaultdict(list)
    for i, r in enumerate(rows):
        if r["street_address"] and r["phone_number"]:
            key = (r["street_address"].strip().lower(), r["phone_number"])
            by_addr_phone[key].append(i)

    remove = set()
    merged_pairs = []

    for key, idxs in by_addr_phone.items():
        if len(idxs) < 2:
            continue
        personal = [i for i in idxs if is_bare_personal_name(rows[i]["law_firm_name"])]
        branded = [i for i in idxs if is_firm_branded(rows[i]["law_firm_name"])]
        for pi in personal:
            if pi in remove:
                continue
            candidates = surname_candidates(rows[pi]["law_firm_name"])
            if not candidates:
                continue
            for bi in branded:
                if bi == pi or bi in remove:
                    continue
                branded_lower = rows[bi]["law_firm_name"].lower()
                matched_surname = next((s for s in candidates if s in branded_lower), None)
                if not matched_surname:
                    continue
                if matched_surname in _COMMON_SURNAMES:
                    # Require a first-name token to also appear — a bare
                    # common surname isn't enough evidence alone.
                    first_names = [t for t in _tokens(rows[pi]["law_firm_name"]) if len(t) >= 3 and t != matched_surname]
                    if not any(t in branded_lower for t in first_names):
                        continue
                # Backfill any field the branded row is missing from the
                # personal row before dropping the personal row.
                for field in ("website", "email", "phone_number", "street_address", "zip_code"):
                    if not rows[bi][field] and rows[pi][field]:
                        rows[bi][field] = rows[pi][field]
                remove.add(pi)
                merged_pairs.append((rows[pi]["law_firm_name"], rows[bi]["law_firm_name"]))
                break

    kept = [r for i, r in enumerate(rows) if i not in remove]
    kept.sort(key=lambda r: (r["city"], r["law_firm_name"]))

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(kept)

    print(f"Merged {len(remove)} personal-name rows into their firm-branded row:")
    for a, b in merged_pairs:
        print(f"  {a!r} -> {b!r}")
    print(f"Final: {len(kept)} rows (was {len(rows)})")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 dedupe_person_firm.py <slug>")
        sys.exit(1)
    main(sys.argv[1])
