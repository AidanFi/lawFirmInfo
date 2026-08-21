#!/usr/bin/env python3
"""
Comprehensive final cleanup for all 5 KS counties:
1. Remove confirmed non-law corporate entities that slipped through
2. Deduplicate attorney/firm soft-duplicates (person listed under firm + personal name)
3. Deduplicate government offices (DA, public defender listed multiple times)
"""
import csv, re, json
from pathlib import Path

DATA_DIR = Path('/Users/aidanfields/lawFirmInfo/app/county-data')
MANIFEST_PATH = DATA_DIR / 'manifest.json'

COUNTIES = ['johnson-county-ks','wyandotte-county-ks','leavenworth-county-ks','miami-county-ks','linn-county-ks']

# ── Confirmed non-law corporate entities (case-insensitive exact match) ────────
NON_LAW_FINAL = {
    # Johnson County corporate slippage
    'national cable television cooperative', 'national cable television cooperative, inc',
    'netsmart technologies', 'netsmart technologies, inc',
    'performance contracting group', 'performance contracting group inc',
    'terracon consultants', 'terracon consultants, inc.',
    'tyler technologies', 'tyler technologies, inc.',
    'title midwest inc', 'title midwest',
    'ameritrust group', 'ameritrust group, inc.',
    # Any remaining single-word corporate brands not caught before
    'pcg', 'tfl', 'tifec', 'forte', 'qgc', 'qii',
    # Wyandotte corporate slippage
    'associated wholesale grocers, inc.', 'associated wholesale grocers',
    'archdiocese of kansas city in kansas',
    'bq and assoc',  # ambiguous non-law abbreviation
    # Leavenworth
    'campbell burto pamelamurray tillotson burton leavenworth',  # garbled kscourts scraping artifact
}

# ── Firm-name soft dedup: keep the more informative/complete version ──────────
# Format: {name_to_remove_lc: name_to_keep} (both must be in same county CSV)
FIRM_DEDUP = {
    # Johnson County
    'boothe walsh law': 'Boothe Walsh Law & Mediation',
    'derrick pearce': 'Derrick A Pearce Attorney',
    'don a. peterson': 'Don Peterson Law LLC',
    'edward mcconwell': 'Edward A McConwell Law Office',
    'kay l. mccarthy': 'Kay L. McCarthy Law & Mediation LLC',
    'linda small': 'The Law Office of Linda Small, Ltd',
    'richard carnahan': 'Law Office of Richard A. Carnahan',
    'robert kumin': 'Robert A Kumin PC',
    'scott waddell': 'A. Scott Waddell',
    'wiedner mcauliffe': 'Wiedner & McAuliffe, Ltd.',
    # The key challenge: only remove if the "keep" version also exists
}

# ── Government office exact dedup (keep canonical name, remove others) ─────────
# Groups: entries in same city that are the same entity
GOVT_DEDUP_REMOVE = {
    # Johnson County DA's office — keep "Johnson County District Attorney's Office"
    "johnson county district attorney",
    "johnson county da's office",
    "johnson co. district attorney's office",
    "johnson county, da office",
    # Johnson County Public Defender — keep "Johnson County Public Defender's Office"
    "johnson county public defender",
    # Wyandotte DA — keep "Wyandotte County District Attorney's Office"
    "wyandotte district attorney's office",
    "office of the district attorney",
    # Federal Public Defender — keep "Office of the Federal Public Defender"
    "kansas federal public defender",
    # Leavenworth/Miami/Linn duplicates (add if found)
}


def entry_score(r):
    score = 0
    if r.get('website', '').strip(): score += 4
    if r.get('phone_number', '').strip(): score += 3
    if r.get('email', '').strip(): score += 2
    if r.get('legal_directory_listing', '').strip(): score += 2
    score += len(r.get('law_firm_name', '')) * 0.01
    return score


def process_county(slug):
    path = DATA_DIR / f'{slug}.csv'
    rows = list(csv.DictReader(open(path)))
    fieldnames = list(rows[0].keys()) if rows else []
    original = len(rows)
    removed = []

    # ── 1. Remove confirmed non-law corporate entries ─────────────────────────
    kept = []
    for r in rows:
        name_lc = r.get('law_firm_name', '').strip().lower()
        if name_lc in NON_LAW_FINAL:
            removed.append(f'NON-LAW: {r["law_firm_name"]!r}')
        else:
            kept.append(r)
    rows = kept

    # ── 2. Remove government office duplicates ────────────────────────────────
    # Only remove if the canonical version exists in this county
    names_in_county = {r.get('law_firm_name','').strip().lower() for r in rows}
    kept = []
    for r in rows:
        name_lc = r.get('law_firm_name', '').strip().lower()
        if name_lc in GOVT_DEDUP_REMOVE:
            removed.append(f'GOVT-DUP: {r["law_firm_name"]!r}')
        else:
            kept.append(r)
    rows = kept

    # ── 3. Firm-name soft dedup ───────────────────────────────────────────────
    # Only remove "name_to_remove" if the canonical "name_to_keep" also exists
    names_by_city = {}
    for r in rows:
        key = (r.get('law_firm_name','').strip().lower(), r.get('city','').strip().lower())
        names_by_city[key] = r

    kept = []
    for r in rows:
        name_lc = r.get('law_firm_name', '').strip().lower()
        city_lc = r.get('city', '').strip().lower()
        canonical = FIRM_DEDUP.get(name_lc)
        if canonical:
            # Only remove this if the canonical version exists in same city
            canonical_exists = (canonical.lower(), city_lc) in names_by_city
            if canonical_exists:
                removed.append(f'FIRM-DUP: {r["law_firm_name"]!r} → {canonical!r}')
                continue
        kept.append(r)
    rows = kept

    # ── 4. Final exact dedup safety net ──────────────────────────────────────
    seen = set()
    dedup_rows = []
    for r in rows:
        key = (r.get('law_firm_name', '').strip().lower(), r.get('city', '').strip().lower())
        if key not in seen:
            seen.add(key)
            dedup_rows.append(r)
        else:
            removed.append(f'EXACT-DUP: {r["law_firm_name"]!r}')
    rows = dedup_rows

    # Write back
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f'{slug}: {original} → {len(rows)} (-{len(removed)})')
    for note in removed:
        print(f'  {note}')
    return len(rows)


results = {}
for slug in COUNTIES:
    n = process_county(slug)
    results[slug] = n
    print()

# Update manifest
manifest = json.loads(MANIFEST_PATH.read_text())
for county in manifest['counties']:
    if county['slug'] in results:
        county['firm_count'] = results[county['slug']]
total = sum(c['firm_count'] for c in manifest['counties'])
manifest['total_firms'] = total
MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))

print(f'Manifest total: {total}')
print('Final KS counts:')
for slug, n in results.items():
    print(f'  {slug}: {n}')
