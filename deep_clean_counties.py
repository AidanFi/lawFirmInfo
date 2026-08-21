#!/usr/bin/env python3
"""Deep quality audit + cleanup for Jackson, Greene, and St. Charles County CSVs."""

import csv
import json
import re
import sys
from pathlib import Path

# ── helpers ──────────────────────────────────────────────────────────────────

_LAW_INDICATORS = re.compile(
    r'\b(?:law|legal|attorney|attorneys|lawyer|lawyers|counsel|counselor|'
    r'solicitor|barrister|esquire|esq\.?|llp|pllc|apc|p\.c\.|p\.a\.|'
    r'notary|paralegal|litigation|advocate|defender|defense|litigat|'
    r'injury|accident|criminal|divorce|bankruptcy|immigration|probate|'
    r'estate planning|trial|courthouse|firm)\b',
    re.IGNORECASE,
)

_NON_LAW_PATTERNS = re.compile(
    r'\b(?:'
    # Auto / vehicle
    r'auto parts|autoparts|napa auto|o\'?reilly|advance auto|carquest|'
    r'auto paint|paint.?body|powder coat|sandblasting|truck parts|'
    r'car wash|tire|muffler|transmission|pawn|thrift shop|appliance parts|'
    r'firmo coral|hulco|wire guy|'
    # Food / drink / entertainment
    r'restaurant|bistro|grill|grille|catering|pantry|bakery|cafe|tavern|'
    r'bar & grill|steakhouse|steak.{1,8}seafood|seafood|patisserie|'
    r'pizza|taco|burrito|sushi|diner|pub|brewery|winery|vineyard|bowling|'
    r'park lanes|parlor|catering|country club|golf|marina|paddle|'
    r'paintball|combat academy|escape room|'
    # Financial / insurance / real estate
    r'financial advisor|financial planning|investment|securities|'
    r'insurance agent|healthmarkets|health insurance|medicare|'
    r'loan officer|mortgage|realtor|real estate|realty|property mgmt|'
    r'accounting|accountant|bookkeeping|cpa|tax preparation|tax prep|'
    r'payroll|credit counseling|credit repair|'
    # Medical / health / personal care
    r'counseling|therapist|therapy|chiropractic|chiropractor|physical therapy|'
    r'dental|dentist|medical|clinic|hospital|pharmacy|veterinary|vet clinic|'
    r'hair salon|barber|nail salon|spa|massage|yoga|fitness|gym|'
    r'day care|child care|learning academy|preschool|'
    # Religious / nonprofit
    r'church|parish|cathedral|mosque|synagogue|temple|ministry|'
    r'christian|baptist|catholic|methodist|'
    # Government / civic
    r'police|sheriff|fire dept|highway patrol|dmv|driver.?s exam|'
    r'city hall|county clerk|court clerk|'
    # Misc services
    r'nursery|garden center|florist|landscaping|paving|concrete|'
    r'roofing|plumbing|hvac|electrical contractor|painting contractor|'
    r'cleaning service|janitorial|storage|moving|trucking|'
    r'music lessons|photography|marketing|digital agency|'
    r'staffing|recruiting|'
    r'adult day care|senior care|assisted living|'
    r'parking|valet|'
    r'auction|auctioneers|'
    r'gun.?pawn|pawn shop|'
    r'package store|liquor|'
    r'steel|fabricat|manufacturing|'
    r'faith|spiritual|path at unity|'
    r'divorce recovery'
    r')\b',
    re.IGNORECASE,
)

# Exact-name kills (lower-cased)
_NON_LAW_EXACT = {
    # Jackson County
    "business credit works",
    "car accident lawyer kansas city",  # fake seo page, no real firm
    "kansas city personal injury lawyer",  # same
    "stewart n processing llc",
    "the house of kensington llc",
    "the wire guy's llc",
    "baba's pantry",
    "bella patina",
    "cash america pawn",
    "el patron",
    "grass pad",
    "hillcrest thrift shop",
    "hillside parlor",
    "iglesia cristiana palabra viva",
    "kimball appliance parts",
    "laz parking",
    "lawson steel",
    "marina 27 steak & seafood",
    "midlife divorce recovery",
    "nathan lawrence music lessons",
    "national pawn",
    "national truck parts",
    "pinch catering",
    "platinum paving",
    "sherwin-williams",
    "spirit path at unity village",
    "hulco parts",
    "firmo corals",
    "t williams & associates",  # accounting firm
    # Greene County
    "forman auction service",
    "m&m gun & pawn",
    "performance paint & chassis",
    "j and d package store",
    "legacy early learning academy",
    "security finance",
    "ball paving",
    "benjamin f. edwards",
    "ed derr personal counseling",
    "enterprise park lanes",
    "lawing financial",
    "orchard park apartments",
    "palm & paddle grille",
    "1 gib agency",
    "cityscape",
    "citystage",
    "coast to coast",
    "fogdog",
    # St. Charles County
    "lezo accounting services",
    "acheck21",
    "achcheck21",
    "corey bowman",
    "maracas",
    "willpower digital",
    "wacky warriors paintball",
    "eastern missouri police academy",
    "leo lawrence real estate",
    "assumption parish",
    "bob & paul's nursery",
    "chandler hill vineyards",
    "clearpoint credit counseling",
    "frisella nursery",
    "gary patton service co.",
    "gary patton service co",
    "purler combat academy",
    "brian russell paasch",
    "catlett & associates",
    "marty saladin",
    "lawlor corporation",
    "open doors adult day care",
    "missouri state highway patrol drivers exam office",
    "spott free parking area",
    "o'reilly auto parts",
    "advance auto parts",
    "napa auto parts",
    "carquest auto parts",
    "a-1 paint powder and sandblasting",
    "menzie's auto paint & body",
    # Duplicates / SEO junk
    "sumner law group",  # 314 area code in Jackson County — St. Louis firm
}

# Jackson County valid ZIPs
JACKSON_ZIPS = {
    "64101","64102","64103","64104","64105","64106","64107","64108",
    "64109","64110","64111","64112","64113","64114","64120",
    "64123","64124","64125","64126","64127","64128","64129",
    "64130","64131","64132","64133","64134","64136","64137",
    "64138","64139","64145","64146","64147","64148","64149",
    "64050","64052","64053","64054","64055","64056","64057","64058",
    "64013","64014","64015","64016",
    "64063","64064","64065",
    "64082","64086",
    "64029","64030","64075","64070",
}

# ZIPs that are NOT Jackson County
JACKSON_BAD_ZIPS = {"64116","64118","64150","64151","64152","64153","64154","64155","64157","64158","64163","64164","64165","64166","64167","64168"}

def is_non_law(name: str) -> bool:
    n = name.strip().lower()
    if n in _NON_LAW_EXACT:
        return True
    if _NON_LAW_PATTERNS.search(name):
        return True
    return False

def has_law_indicator(name: str) -> bool:
    return bool(_LAW_INDICATORS.search(name))

def no_contact(row: dict) -> bool:
    return not row.get("phone_number") and not row.get("email") and not row.get("website")

# ── per-county logic ──────────────────────────────────────────────────────────

def filter_jackson(rows):
    kept, removed = [], []
    for r in rows:
        name = r["law_firm_name"]
        zip_code = (r.get("zip_code") or "").strip()

        # Out-of-county ZIPs (Clay/Platte counties)
        if zip_code in JACKSON_BAD_ZIPS:
            removed.append(("out-of-county ZIP", name, zip_code))
            continue

        # Non-law name match
        if is_non_law(name):
            removed.append(("non-law name", name, ""))
            continue

        # No contact at all
        if no_contact(r):
            removed.append(("no contact", name, ""))
            continue

        kept.append(r)
    return kept, removed

def filter_greene(rows):
    kept, removed = [], []
    for r in rows:
        name = r["law_firm_name"]

        if is_non_law(name):
            removed.append(("non-law name", name, ""))
            continue

        if no_contact(r):
            removed.append(("no contact", name, ""))
            continue

        kept.append(r)
    return kept, removed

def filter_st_charles(rows):
    kept, removed = [], []
    for r in rows:
        name = r["law_firm_name"]

        if is_non_law(name):
            removed.append(("non-law name", name, ""))
            continue

        if no_contact(r):
            removed.append(("no contact", name, ""))
            continue

        kept.append(r)
    return kept, removed

# ── additional borderline check ───────────────────────────────────────────────

# Names with no law indicator that we've manually verified are NOT law firms
MANUAL_NON_LAW = {
    # Jackson
    "e-z food mart", "dollar general", "walmart", "target", "sam's club",
    "price chopper", "hen house", "hy-vee", "dillons", "kroger",
    "enterprise rent-a-car", "hertz", "avis", "budget rent a car",
    "jiffy lube", "valvoline", "firestone", "goodyear", "pep boys",
    "midas", "monro muffler",
    # Generic financial
    "edward jones", "raymond james", "merrill lynch", "morgan stanley",
    "ameriprise financial", "northwestern mutual", "new york life",
    "farmers insurance", "state farm", "allstate", "liberty mutual",
    "nationwide", "progressive insurance",
    # Medical
    "saint luke's", "st. luke's", "truman medical", "children's mercy",
    "research medical", "overland park regional",
    # Retail / misc
    "home depot", "lowe's", "menards", "ace hardware", "true value",
    "office depot", "staples", "fedex office", "ups store",
    "h&r block", "jackson hewitt",
    "re/max", "keller williams", "century 21", "coldwell banker",
    "berkshire hathaway homeservices",
}

def manual_non_law_check(name: str) -> bool:
    return name.strip().lower() in MANUAL_NON_LAW

# ── I/O ───────────────────────────────────────────────────────────────────────

def load_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def save_csv(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

def update_manifest(slug, count):
    mpath = Path("app/county-data/manifest.json")
    data = json.loads(mpath.read_text())
    from datetime import date
    today = date.today().isoformat()
    for c in data["counties"]:
        if c["slug"] == slug:
            c["firm_count"] = count
            c["last_updated"] = today
            break
    mpath.write_text(json.dumps(data, indent=2) + "\n")

# ── main ──────────────────────────────────────────────────────────────────────

COUNTIES = [
    {
        "slug": "jackson-county-mo",
        "filter": filter_jackson,
    },
    {
        "slug": "greene-county-mo",
        "filter": filter_greene,
    },
    {
        "slug": "st-charles-county-mo",
        "filter": filter_st_charles,
    },
]

for county in COUNTIES:
    slug = county["slug"]
    path = Path(f"app/county-data/{slug}.csv")
    rows = load_csv(path)
    fieldnames = list(rows[0].keys()) if rows else []
    before = len(rows)

    # Apply manual non-law check on top of each filter
    for r in rows:
        pass  # integrated below via filter functions which call is_non_law → checks MANUAL_NON_LAW too

    kept, removed = county["filter"](rows)

    # Also apply manual non-law check
    final_kept = []
    for r in kept:
        n = r["law_firm_name"]
        if manual_non_law_check(n):
            removed.append(("manual non-law", n, ""))
        else:
            final_kept.append(r)

    after = len(final_kept)
    save_csv(path, final_kept, fieldnames)
    update_manifest(slug, after)

    print(f"\n=== {slug} ===")
    print(f"  Before: {before}  After: {after}  Removed: {before - after}")
    by_reason = {}
    for reason, name, extra in removed:
        by_reason.setdefault(reason, []).append(f"{name}{' ('+extra+')' if extra else ''}")
    for reason, names in by_reason.items():
        print(f"  [{reason}] {len(names)} entries:")
        for nm in sorted(names):
            print(f"    - {nm}")

print("\nDone. manifest.json updated.")
