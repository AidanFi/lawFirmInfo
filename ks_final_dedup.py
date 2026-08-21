#!/usr/bin/env python3
"""
Final dedup + remaining keyword dump removal for all 5 KS counties.

Pass 1: Remove keyword dumps missed by previous passes (City, KS Practice Area Lawyer format)
Pass 2: Soft-dedup person names (same first+last+city, different middle/format)
Pass 3: Soft-dedup firm names (same normalized firm + city)
Pass 4: Dedup government offices (DA offices, public defenders)
"""
import csv, re, json
from pathlib import Path
from collections import defaultdict

DATA_DIR = Path('/Users/aidanfields/lawFirmInfo/app/county-data')
MANIFEST_PATH = DATA_DIR / 'manifest.json'

COUNTIES = ['johnson-county-ks','wyandotte-county-ks','leavenworth-county-ks','miami-county-ks','linn-county-ks']

# ── Pattern matchers ──────────────────────────────────────────────────────────

# Extended keyword dump: City, KS [Practice Area] [Lawyer|Attorney]
#   These have 1-2 spaces between words (source formatting artifact)
KW_DUMP3 = re.compile(
    r'^[A-Za-z\s\-]+,\s*ks\s+\w[\w\s]*\s+(lawyer|attorney)\s*$',
    re.IGNORECASE
)

LAW_IND = re.compile(
    r'\b(law|legal|attorney|attorneys|counsel|llp|pllc|p\.c\.|p\.a\.|pa\b|esq|firm|mediator|arbitr|litigat|paralegal|abogad|j\.d\.|jd\b)\b',
    re.I
)

# Common legal suffixes to strip before name comparison
LEGAL_SUFFIX = re.compile(
    r'\b(jr\.?|sr\.?|ii|iii|iv|esq\.?|j\.d\.?|jd\.?|llc\.?|pllc\.?|p\.c\.?|p\.a\.?|pa\.?|atty\.?|chartered\.?)\b',
    re.I
)

def parse_person_name(name):
    """
    Try to parse a name string as (first, middle_initial, last, city_key).
    Returns None if it doesn't look like a person name.
    """
    # Strip legal suffixes
    cleaned = LEGAL_SUFFIX.sub('', name).strip().strip(',').strip()

    # If it still has law indicators other than name-like patterns, skip
    if re.search(r'\b(law|legal|attorneys|counsel|llp|pllc|mediator|arbitr|litigat|office|group|services|solutions)\b', cleaned, re.I):
        return None

    # "Last, First [MI]" format (kscourts)
    m = re.match(r'^([A-Za-z\'\-]+),\s+([A-Za-z]+)(?:\s+([A-Za-z]))?$', cleaned.strip())
    if m:
        last = m.group(1).lower()
        first = m.group(2).lower()
        mid = m.group(3).lower() if m.group(3) else None
        return (first, mid, last)

    # "First [MI.] Last" or "First Middle Last" or "First Last"
    parts = [p.strip('.') for p in cleaned.split() if p.strip('.')]
    if not parts or len(parts) > 5:
        return None
    # All parts must look like name words (alpha, hyphens, apostrophes)
    if not all(re.match(r'^[A-Za-z\'\-]+$', p) for p in parts):
        return None
    # Need at least 2 parts
    if len(parts) < 2:
        return None

    last = parts[-1].lower()
    first = parts[0].lower()

    # Middle initial or middle name
    mid = None
    if len(parts) == 3:
        mid = parts[1][0].lower()  # just initial of middle
    elif len(parts) == 4:
        # Could be First Middle Middle Last or First Middle Last Suffix
        mid = parts[1][0].lower()

    return (first, mid, last)

def entry_score(r):
    """Score an entry by how much useful info it has (higher = keep this one)."""
    score = 0
    if r.get('website','').strip(): score += 4
    if r.get('phone_number','').strip(): score += 3
    if r.get('email','').strip(): score += 2
    if r.get('legal_directory_listing','').strip(): score += 2
    if r.get('google_business_profile','').strip(): score += 1
    # Prefer the more complete name (longer)
    score += len(r.get('law_firm_name','')) * 0.01
    return score

def normalize_firm(name):
    """Normalize a firm name for comparison."""
    n = name.lower()
    n = re.sub(r'[^\w\s]', ' ', n)
    n = re.sub(r'\b(the|a|an|of|and|&)\b', ' ', n)
    n = re.sub(r'\b(law|office|offices|firm|group|llc|pllc|llp|pc|pa|inc|corp|ltd)\b', ' ', n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n

def normalize_govt(name):
    """Normalize government office name."""
    n = name.lower()
    n = re.sub(r"['\.\-]", '', n)
    n = re.sub(r'\b(johnson|wyandotte|leavenworth|miami|linn)\s+county\b', 'county', n)
    n = re.sub(r'\b(jo\s*co|jo\s+co\.?)\b', 'county', n)
    n = re.sub(r'\b(da|dist\.?\s*atty\.?|district\s+attorney)\b', 'district_attorney', n)
    n = re.sub(r'\b(public\s+defender|pd|indigents\s+defense)\b', 'public_defender', n)
    n = re.sub(r'\b(prosecutor|prosecuting)\b', 'prosecutor', n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n

GOVT_KEYWORDS = re.compile(
    r'\b(district\s+attorney|public\s+defender|prosecutor|da\'?s?\s+office|'
    r'indigents\s+defense|county\s+attorney|sbids)\b',
    re.I
)

def process_county(slug):
    path = DATA_DIR / f'{slug}.csv'
    rows = list(csv.DictReader(open(path)))
    fieldnames = list(rows[0].keys()) if rows else []
    original = len(rows)
    removed_kw = 0
    removed_person_dup = 0
    removed_firm_dup = 0
    removed_govt_dup = 0

    # ── Pass 1: Keyword dump removal ──────────────────────────────────────────
    kept = []
    for r in rows:
        name = r.get('law_firm_name','').strip()
        if KW_DUMP3.match(name):
            removed_kw += 1
        else:
            kept.append(r)
    rows = kept

    # ── Pass 2: Person-name soft dedup ────────────────────────────────────────
    # Group entries that parse to same (first, last, city)
    person_groups = defaultdict(list)
    non_person = []
    for r in rows:
        name = r.get('law_firm_name','').strip()
        city = r.get('city','').strip().lower()
        parsed = parse_person_name(name)
        if parsed:
            key = (parsed[0], parsed[2], city)  # (first, last, city)
            person_groups[key].append(r)
        else:
            non_person.append(r)

    person_kept = []
    for key, group in person_groups.items():
        if len(group) == 1:
            person_kept.extend(group)
            continue
        # Multiple entries for same first+last+city — check middle names are compatible
        # Sort by score descending; keep highest-scored one
        # But first verify they really are the same person (middle initial must not conflict)
        middles = set()
        for r in group:
            parsed = parse_person_name(r.get('law_firm_name',''))
            if parsed and parsed[1]:
                middles.add(parsed[1])
        # If two different middle initials, they're different people — keep all
        if len(middles) > 1:
            person_kept.extend(group)
            continue
        # Same or compatible — keep best, discard rest
        group.sort(key=entry_score, reverse=True)
        person_kept.append(group[0])
        removed_person_dup += len(group) - 1

    rows = person_kept + non_person

    # ── Pass 3: Government office dedup ──────────────────────────────────────
    # Group DA offices, public defender offices, etc.
    govt_groups = defaultdict(list)
    non_govt = []
    for r in rows:
        name = r.get('law_firm_name','').strip()
        city = r.get('city','').strip().lower()
        if GOVT_KEYWORDS.search(name):
            key = (normalize_govt(name), city)
            govt_groups[key].append(r)
        else:
            non_govt.append(r)

    govt_kept = []
    for key, group in govt_groups.items():
        if len(group) == 1:
            govt_kept.extend(group)
            continue
        group.sort(key=entry_score, reverse=True)
        govt_kept.append(group[0])
        removed_govt_dup += len(group) - 1

    rows = govt_kept + non_govt

    # ── Pass 4: Exact dedup (safety net) ──────────────────────────────────────
    seen = set()
    dedup_rows = []
    for r in rows:
        key = (r.get('law_firm_name','').strip().lower(), r.get('city','').strip().lower())
        if key not in seen:
            seen.add(key)
            dedup_rows.append(r)
    exact_dups = len(rows) - len(dedup_rows)
    rows = dedup_rows

    # Write back
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    total_removed = original - len(rows)
    print(f'{slug}: {original} → {len(rows)} (-{total_removed} total: '
          f'{removed_kw} kw-dumps, {removed_person_dup} person-dups, '
          f'{removed_govt_dup} govt-dups, {exact_dups} exact-dups)')
    return len(rows)


results = {}
for slug in COUNTIES:
    results[slug] = process_county(slug)

# Update manifest
manifest = json.loads(MANIFEST_PATH.read_text())
for county in manifest['counties']:
    if county['slug'] in results:
        county['firm_count'] = results[county['slug']]
total = sum(c['firm_count'] for c in manifest['counties'])
manifest['total_firms'] = total
MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))

print(f'\nManifest total: {total}')
print('\nFinal counts:')
for slug, n in results.items():
    print(f'  {slug}: {n}')
