#!/usr/bin/env python3
"""
Final pass: keep ONLY entries for actual law firms / solo practitioners.
Remove government agencies, in-house corporate counsel employers, bankruptcy trustees,
retired attorneys, non-law businesses, and residual keyword dumps.
"""
import csv, re, json
from pathlib import Path

DATA_DIR = Path('/Users/aidanfields/lawFirmInfo/app/county-data')
MANIFEST_PATH = DATA_DIR / 'manifest.json'
COUNTIES = ['johnson-county-ks','wyandotte-county-ks','leavenworth-county-ks','miami-county-ks','linn-county-ks']

# ── Exact-match removals (lowercase) ─────────────────────────────────────────
REMOVE_EXACT = {
    # Federal government agencies
    'dept of homeland security, uscis',
    'u.s. department of homeland security',
    'epa region 7 hq', 'us epa region 7',
    'environmental protection agency (epa) region 7',
    'u.s. environmental protection agency',
    'united states environmental protection agency, region 7',
    'us environmental protection agency, region 7',
    'us food and drug administration',
    'u.s. food and drug administration',
    'u.s. equal employment opportunity commission',
    'u.s. doj', 'u.s. dept. of justice',
    'u.s. court, district of kansas',
    'u.s. courthouse',
    'united states government chief us probation officer',
    'dist. ct. for the dist. of ks',

    # State/local government agencies
    'ks dept of labor, workers comp div',
    "ks dept of labor-div of worker's compensation",
    'kansas department for children and families',
    'kansas department of labor',
    'johnson county board of county commissioners',
    'water dist. no 1 - johnson county',
    'olathe public schools usd #233',
    'unified gov. of wyandotte co. kck.',
    'unified gov. of wyandotte county/kansas city',
    'unified gov. of wyandotte county and kck',
    'unified government of wyandotte county',
    'unified government of wyandotte county/kansas city, kansas',

    # DA offices, public defenders, indigent defense (government employees, not law firms)
    "johnson co. d.a.'s office",
    "johnson county public defender's office",
    'johnson county district attorney',
    "johnson county district attorney's office",
    "johnson county da's office",
    "johnson county, da office",
    "johnson co. district attorney's office",
    'kansas state board of indigent defense services',
    'kansas board of indigents\' defense services',
    "the kansas state board of indigents' defense services",
    "kansas state board of indigents' defense services",
    "state board of indigents' defense services",
    "the kansas board of indigents' defense services",
    "wyandotte county public defender's office",
    'wyanotte county public defender sbids',
    'office of the federal public defender',
    "wyandotte county district attorney's office",
    'leavenworth county district attorney',
    "leavenworth district court",
    "miami county district attorney",
    '6th judicial district - linn county',

    # Court / trustee positions (not law firms)
    'division m4, johnson county courthouse',
    'chapter 13 trustee',
    'w h griffin trustee', 'griffin w h trustee',
    'william h griffin chapter 13',
    'william h griffin chapter 13 trustee',

    # Non-law corporations & employers
    'black & veatch',
    'kiewit corp.',
    'seaboard overseas and trading group',
    'watco companies, l.l.c.',
    'carrier logistics, llc',
    'd & l transport',
    'dm gary holding co.',
    'm3sixty administration',
    'quest analytics, l.l.c.',
    'benefit trust co.',
    'united health group co: optum rx',
    'zurich services corp.',
    'choice solutions, l.l.c.',
    'q services',
    'k c hopps',
    'bradley strat. & slns',

    # Nonprofit / advocacy (not law firms)
    'american academy of family physicians',
    'american civil liberties union of kansas',
    'united community services of johnson county',

    # Retired / inactive
    'dana harris (retired)',

    # Wyandotte non-law
    'life care center of kansas city, kansas',
    'kansas department for children and families',  # duplicate across counties
}

# ── Pattern-based removal ─────────────────────────────────────────────────────

# Remaining keyword dumps not caught before
KW_DUMP = re.compile(
    r'(,\s*ks\s+\w[\w\s]*\s+(lawyer|attorney)\s*$)'   # City, KS [PracticeArea] Lawyer
    r'|(^([\w\s]+,\s*ks\s+lawyer)\s*$)',               # City, KS Lawyer
    re.IGNORECASE
)

# Government agency patterns (catch variants not in exact list)
GOVT_AGENCY = re.compile(
    r'\b(u\.s\.\s+environmental\s+protection|united\s+states\s+environmental\s+protection|'
    r'u\.s\.\s+department\s+of|united\s+states\s+department\s+of|'
    r'dept\.\s+of\s+homeland|department\s+of\s+homeland|'
    r'food\s+and\s+drug\s+administration|'
    r'equal\s+employment\s+opportunity\s+commission|'
    r'unified\s+government\s+of\s+wyandotte|'
    r'school\s+district|unified\s+school\s+district|usd\s+#\d|'
    r'county\s+board\s+of\s+(county\s+)?commissioners|'
    r'water\s+dist(rict)?\.?\s+no\.?\s+\d|'
    r'indigents?\s+defense\s+services)\b',
    re.I
)

# Corporate employer patterns (non-law businesses)
CORP_EMPLOYER = re.compile(
    r'\b(engineering\s+(company|corp|inc|group|firm)|'
    r'construction\s+(company|corp|inc|group)|'
    r'logistics,?\s+(llc|inc|corp)|transport(ation)?,?\s+(llc|inc|corp)|'
    r'manufacturing,?\s+(llc|inc|corp)|'
    r'holding\s+co\.|holdings,?\s+(llc|inc)|'
    r'administration,?\s+(llc|inc)|'
    r'analytics,?\s+(llc|inc|l\.l\.c\.)|'
    r'services\s+corp\.)\b',
    re.I
)

# Law firm indicator — entries with ANY of these are KEPT regardless
LAW_FIRM_IND = re.compile(
    r'\b(law|legal|attorney|attorneys|atty\b|aty\b|counsel|llp|pllc|lllp|pllp|mediati|'
    r'arbitr|litigat|paralegal|abogad|j\.d\.|jd\b|esq|barrister|solicitor|'
    r'chartered\b|chtd)\b'
    r'|p\.c\.|p\.a\.|l\.c\.|l\.l\.p\.|l\.l\.c\.|lpa\b',
    re.I
)

# Person-name patterns — entries that look like attorneys' names are KEPT
# "First [MI] Last", "First [Middle] Last", "Last, First [MI]", with optional Jr/Sr/II/III
PERSON_NAME = re.compile(
    r'^([A-Z][a-z\'\-]+\.?'               # First name (or initial)
    r'(\s+[A-Z]\.?)?'                      # Optional middle initial
    r'(\s+[A-Z][a-z\'\-]+){1,3}'          # Last name (and optionally more)
    r'(\s*,?\s*(jr\.?|sr\.?|ii|iii|iv))?' # Optional Jr/Sr/II/III
    r'\s*$)',
    re.I
)
KSCOURTS_NAME = re.compile(  # Last, First [MI] [Jr/Sr]
    r'^[A-Za-z\'\-]+,\s+[A-Za-z]+(\s+[A-Za-z]\.?)?(\s+(jr\.?|sr\.?|ii|iii|iv))?$'
)
# "First Name Jr." style with suffix
NAME_WITH_SUFFIX = re.compile(
    r'^[A-Za-z][a-z\'\-]+(\s+[A-Za-z]\.?)?(\s+[A-Za-z][a-z\'\-]+){1,3}\s+(jr\.?|sr\.?|ii|iii|iv)\s*\.$',
    re.I
)

def is_person_name(name):
    """Return True if the entry looks like a solo attorney's name."""
    n = name.strip()
    # Remove common attorney suffixes before matching
    cleaned = re.sub(r'\s*,?\s*(jr\.?|sr\.?|ii|iii|iv|esq\.?|jd\.?|j\.d\.?|atty\.?|aty\.?)\s*$', '', n, flags=re.I).strip()
    if PERSON_NAME.match(cleaned):
        return True
    if KSCOURTS_NAME.match(n):
        return True
    # Handles "E. Todd Hottman", "J. Ryan Erker", "D. Matthew Keane"
    if re.match(r'^[A-Z]\.\s+[A-Za-z]+\s+[A-Za-z\'\-]+$', n):
        return True
    # "Charles E., III Fowler" style (kscourts artifact)
    if re.match(r'^[A-Za-z][a-z\'\-]+(\s+[A-Z]\.?,\s+(jr\.?|sr\.?|ii|iii|iv))?\s+[A-Za-z\'\-]+$', n, re.I):
        return True
    return False

def is_law_entity(name):
    """Return True if name clearly indicates a law firm entity (not just a person)."""
    # Has law-firm suffix at end
    if re.search(r'\b(p\.a\.|p\.c\.|l\.c\.|l\.l\.p\.|lpa|pllp|lllp)\s*$', name, re.I):
        return True
    # Multi-partner firm (two or more surnames joined with &)
    if re.search(r'[A-Za-z]\s*&\s*[A-Za-z]', name) and not re.search(r'\b(inc\.|corp\.|company\b)', name, re.I):
        return True
    return False

def should_remove(r):
    name = r.get('law_firm_name', '').strip()
    name_lc = name.lower()
    web = r.get('website', '').strip()

    # 1. Exact match removal list
    if name_lc in REMOVE_EXACT:
        return 'EXACT-MATCH'

    # 2. Keyword dump
    if KW_DUMP.search(name):
        return 'KW-DUMP'

    # 3. Government agency pattern
    if GOVT_AGENCY.search(name) and not LAW_FIRM_IND.search(name):
        return 'GOVT-AGENCY'

    # 4. Corporate employer pattern (only if no law firm indicators)
    if CORP_EMPLOYER.search(name) and not LAW_FIRM_IND.search(name) and not is_law_entity(name):
        return 'CORP-EMPLOYER'

    return None  # Keep

def process_county(slug):
    path = DATA_DIR / f'{slug}.csv'
    rows = list(csv.DictReader(open(path)))
    fieldnames = list(rows[0].keys()) if rows else []
    original = len(rows)
    removed_log = []
    kept = []

    for r in rows:
        reason = should_remove(r)
        if reason:
            removed_log.append((reason, r.get('law_firm_name','')))
        else:
            kept.append(r)

    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(kept)

    print(f'{slug}: {original} → {len(kept)} (-{len(removed_log)})')
    for reason, name in sorted(removed_log):
        print(f'  [{reason}] {name!r}')
    return len(kept)

results = {}
for slug in COUNTIES:
    n = process_county(slug)
    results[slug] = n
    print()

manifest = json.loads(MANIFEST_PATH.read_text())
for c in manifest['counties']:
    if c['slug'] in results:
        c['firm_count'] = results[c['slug']]
total = sum(c['firm_count'] for c in manifest['counties'])
manifest['total_firms'] = total
MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))

print('Final KS counts:')
for slug, n in results.items():
    print(f'  {slug}: {n}')
print(f'Total all counties: {total}')
