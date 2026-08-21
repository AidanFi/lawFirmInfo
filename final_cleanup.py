#!/usr/bin/env python3
"""
Final cleanup: remove non-law-firm entries from KS county CSVs.
Run AFTER add_lawyer_counts.py completes.
"""
import csv, re
from pathlib import Path

DATA_DIR = Path(__file__).parent / "app/county-data"

# Firm names that are definitively not law firms (case-insensitive exact match)
NON_LAW_EXACT = {
    # Banks / financial
    "umb bank", "farmers bank & trust", "pnc bank, na / pnc real estate",
    "busey bank", "first federal bank of kansas city", "enterprise bank & trust",
    "first business bank", "hawthorn bank", "bank of prairie village", "stearns bank",
    "first state bank & trust", "first option bank", "communityamerica federal credit union",
    "bok financial", "lpl financial", "metlife investment management",
    "mccaffree financial corp", "prime capital investment advisors",
    "caliber financial services, inc.", "leslie rudd investment company",
    "demars pension consulting services, inc.", "federated rural electric insurance exchange",
    # Insurance
    "delta dental of kansas", "relation insurance, inc", "relation insurance services",
    "first american title insurance co", "farmers insurance", "nationwide insurance",
    "travelers insurance co", "fidelity security life insurance", "aeris insurance solutions",
    "standard insurance company", "silac insurance company",
    "federated rural electric insurance exchange",
    # Healthcare / medical
    "university of kansas health system", "the university of kansas health system",
    "shawnee mission medical center", "university of ks hospital authority",
    "university of kansas hospital authority", "university of kansas medical center",
    "the university of kansas cancer center", "university of ks medical center",
    "ku medical center", "the university of kansas heath system",
    "dechra veterinary products", "wildcat veterinary clinic",
    "mckesson corporation (rx savings solutions)",
    # Real estate / non-law
    "first washington realty", "reecenichols real estate", "generations real estate",
    "selling kc real estate", "national realty advisors llc",
    # Education / non-legal
    "johnson county community college", "university of kansas system, inc.",
    "unified school district no. 229", "shawnee mission school district",
    "university of kansas",
    # Financial services / consulting
    "edward jones - financial advisor: tedd m maxfield, cfp®",
    "armstrong consulting", "madden consulting services", "embree consulting",
    # Other non-legal
    "church of the resurrection", "church of the nazarene", "church of the nazarene, inc",
    "impact foundation", "economic opportunity foundation inc",
    "carquest auto parts - desoto auto parts", "westlake hardware, inc.",
    "801 restaurant group",
    "de soto driving under the influence (dui) lawyer/law firm/attorney",
    "st. louis personal injury attorney gretchen myers",
    # Loan services (not law firms)
    "midland loan services, a div of pnc bank na",
    "midland loan services, a division of pnc bank na",
    # Government / court entities (not referral law firms)
    "spring hill city clerk", "city of overland park, kansas", "johnson county district court",
    "city of shawnee, ks prosecutor's office", "johnson county district court trustee's office",
    "lenexa municipal court", "city of olathe", "u.s. department of health and human services",
    "city of olathe prosecutor's office", "city of lenexa", "city of olathe legal departments",
    "city of overland park, prosecutor's office", "city of overland park",
    "johnson county district court trustee", "city of leawood kansas", "city of leawood",
    "city of olathe emergency management", "overland park municipal court",
    "johnson county district court, div 15", "city of shawnee",
    "city of overland park municipal court", "10th judicial district court",
    "johnson county - district court administration", "city of olathe municipal court",
    "city of lenexa, kansas", "district court judge, state of kansas",
    "kansas department of labor", "10th judicial district court of kansas, johnson county",
    # Wyandotte government
    "wyandotte county district court", "united states district court",
    "us district court", "us department of housing and urban development",
    "department of housing and urban development",
    "united states district court for the district of kansas",
    "wyandotte county district court, div 12", "u.s. district court of ks",
    "kansas city kansas municipal court", "u.s. district court- district of kansas",
    "u.s. district court for the district of kansas", "u.s. district court of kansas",
    "us district court probation", "district court trustee 29th judicial district",
    # Leavenworth government
    "leavenworth district court", "district court judge", "district court",
    "leavenworth county district court", "district court of the first judicial district",
    "leavenworth county sheriff's office",
    # Miami government
    "miami county district court", "district court judge 6th judicial district",
    # Linn government
    "6th judicial district - linn county",
}

# Firm names to fix (clean up names or merge duplicates)
# Format: {bad_name: good_name} — if good_name is None, just remove
NAME_FIXES = {
    "contact - o'brien law firm, llc": "O'Brien Law Firm, LLC",
    "contact - barton and burrows, llc": "Barton and Burrows, LLC",
    # Remove duplicate DA's office variants (keep one)
    "wyandotte county district attorneys office": None,  # keep the one with apostrophe
    "wyandotte district attorney's office": None,
}

# Remove entries where name+city is an exact duplicate (keep first occurrence)
def deduplicate(rows):
    seen = set()
    kept = []
    for row in rows:
        key = (row.get("law_firm_name", "").strip().lower(),
               row.get("city", "").strip().lower())
        if key in seen:
            continue
        seen.add(key)
        kept.append(row)
    return kept

# Fix all-caps city names
def fix_city_case(rows):
    CITY_FIXES = {
        "overland park": "Overland Park", "spring hill": "Spring Hill",
        "fairway": "Fairway", "olathe": "Olathe", "lenexa": "Lenexa",
        "leawood": "Leawood", "merriam": "Merriam", "mission": "Mission",
        "gardner": "Gardner", "shawnee": "Shawnee", "prairie village": "Prairie Village",
        "de soto": "De Soto", "edgerton": "Edgerton", "roeland park": "Roeland Park",
        "westwood": "Westwood", "mission hills": "Mission Hills",
        "kansas city": "Kansas City", "bonner springs": "Bonner Springs",
        "edwardsville": "Edwardsville", "leavenworth": "Leavenworth",
        "lansing": "Lansing", "basehor": "Basehor", "tonganoxie": "Tonganoxie",
        "paola": "Paola", "osawatomie": "Osawatomie", "louisburg": "Louisburg",
        "mound city": "Mound City",
    }
    for row in rows:
        city = row.get("city", "").strip()
        fixed = CITY_FIXES.get(city.lower(), city)
        # Also fix all-caps
        if city.isupper() and city.lower() in CITY_FIXES:
            row["city"] = CITY_FIXES[city.lower()]
        elif fixed != city:
            row["city"] = fixed
    return rows

# Remove Card Compliant duplicate (keep the one with website/lawyer count)
def fix_card_compliant(rows):
    card_rows = [r for r in rows if "card compliant" in r.get("law_firm_name", "").lower()]
    if len(card_rows) > 1:
        # Keep the one with a website; remove kscourts duplicate
        best = None
        for r in card_rows:
            if r.get("website", "").strip():
                best = r
                break
        if best:
            # Normalize to one entry
            to_remove = set()
            for r in card_rows:
                if r is not best:
                    to_remove.add(id(r))
            rows = [r for r in rows if id(r) not in to_remove]
    return rows

GOVT_PATTERNS = re.compile(
    r'\b(city clerk|city hall|city of |county clerk|county treasurer|'
    r'department of |dept\. of |public library|fire station|police department|'
    r'sheriff(?!.*(law|attorney|legal))|district court|circuit court|municipal court|probate court|'
    r'judicial district|us district court|u\.s\. district|united states district)\b',
    re.IGNORECASE
)

LAW_INDICATORS = ['law', 'legal', 'attorney', 'attorneys', 'counsel', 'llp', 'pllc',
                  'p.c.', 'p.a.', 'esq', 'mediator', 'mediation', 'arbitr']

def is_govt_non_law(name):
    """Return True if name looks like a government/court entity but NOT a law firm."""
    if any(kw in name.lower() for kw in LAW_INDICATORS):
        return False
    return bool(GOVT_PATTERNS.search(name))

def process_county(slug):
    path = DATA_DIR / f"{slug}.csv"
    if not path.exists():
        print(f"  [skip] {slug}: not found")
        return

    rows = list(csv.DictReader(open(path)))
    original = len(rows)
    fieldnames = list(rows[0].keys()) if rows else []

    # 1. Remove non-law firms (exact match + pattern match)
    before = len(rows)
    rows = [r for r in rows
            if r.get("law_firm_name", "").strip().lower() not in NON_LAW_EXACT
            and not is_govt_non_law(r.get("law_firm_name", "").strip())]
    removed_non_law = before - len(rows)

    # 2. Apply name fixes
    fixed_names = 0
    for row in rows:
        name = row.get("law_firm_name", "").strip()
        fix = NAME_FIXES.get(name.lower())
        if fix is not None:
            row["law_firm_name"] = fix
            fixed_names += 1

    # 3. Remove entries marked for removal (fix=None means remove)
    before = len(rows)
    rows = [r for r in rows
            if NAME_FIXES.get(r.get("law_firm_name", "").strip().lower(), "KEEP") != None]
    removed_name_fixes = before - len(rows)

    # 3b. Fix award-count practice areas
    for row in rows:
        pa = row.get("practice_area", "").strip()
        if re.match(r'^\d+\s+(award|super\s+lawyer)', pa, re.I):
            row["practice_area"] = "General"

    # 4. Fix city case
    rows = fix_city_case(rows)

    # 5. Fix Card Compliant duplicate
    rows = fix_card_compliant(rows)

    # 6. Deduplicate (exact name+city)
    before = len(rows)
    rows = deduplicate(rows)
    removed_dups = before - len(rows)

    # Write back
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  {slug}: {original} → {len(rows)} "
          f"(removed {removed_non_law} non-law, {removed_name_fixes} name-fixed, {removed_dups} dups)")
    return len(rows)

if __name__ == "__main__":
    counts = {}
    for slug in ["johnson-county-ks", "wyandotte-county-ks",
                 "leavenworth-county-ks", "miami-county-ks", "linn-county-ks"]:
        print(f"[{slug}]")
        n = process_county(slug)
        if n:
            counts[slug] = n

    print("\nFinal counts:")
    for slug, n in counts.items():
        print(f"  {slug}: {n}")
