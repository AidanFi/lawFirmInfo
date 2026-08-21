#!/usr/bin/env python3
"""
Johnson County pass-2 cleanup: remove remaining bad entries found after workflow.
"""
import csv, re, json
from pathlib import Path

CSV_PATH = Path('/Users/aidanfields/lawFirmInfo/app/county-data/johnson-county-ks.csv')
MANIFEST_PATH = Path('/Users/aidanfields/lawFirmInfo/app/county-data/manifest.json')

# Definitively non-law single-word / corporate entries (kscourts employer names)
REMOVE_EXACT = {
    # Consulting / non-law
    'acutula consulting, llc', 'creative one marketing, llc', 'motley consulting services llc',
    'quality technology services, llc', 'stephen atha consulting llc',
    # Keyword dumps – city+type fragments
    'leawood auto accident lawyer', 'lenexa auto accident lawyer',
    'mission auto accident lawyer', 'olathe auto accident lawyer',
    # Corporate employers (no law indicators, no website, no source)
    'acertus', 'amynta', 'bokf', 'brightspeed', 'cbre', 'corbion', 'curi', 'dell',
    'esis', 'empower', 'enterprisekc', 'epiq', 'hoopla', 'iqvia', 'lumen',
    'metronet', 'netsmart', 'ppmrrg', 'qgc', 'qii', 'qts', 'safehome',
    'terracon', 'unitedlex', 'waterone', 'wellsky', 'youngwilliams',
    # John Deere
    'john deere ag marketing center',
}

# Pattern: "City, KS Attorney with" / "City, KS Lawyer with" keyword dumps
KW_DUMP = re.compile(
    r'^.{3,50},\s*ks\s+(attorney|lawyer|law firm)\s+with\s*$', re.IGNORECASE
)

rows = list(csv.DictReader(open(CSV_PATH)))
fieldnames = list(rows[0].keys()) if rows else []
original = len(rows)

kept = []
removed = []
for r in rows:
    name = r.get('law_firm_name', '').strip()
    name_lc = name.lower()
    if name_lc in REMOVE_EXACT:
        removed.append(name)
    elif KW_DUMP.match(name):
        removed.append(name)
    else:
        kept.append(r)

print(f'Johnson County: {original} → {len(kept)} (removed {len(removed)})')
print('Removed entries:')
for n in sorted(removed):
    print(f'  - {n!r}')

# Write back
with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(kept)

# Update manifest
manifest = json.loads(MANIFEST_PATH.read_text())
for county in manifest['counties']:
    if county['slug'] == 'johnson-county-ks':
        county['firm_count'] = len(kept)
        break

total = sum(c['firm_count'] for c in manifest['counties'])
manifest['total_firms'] = total
MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))
print(f'\nManifest updated: johnson-county-ks={len(kept)}, total={total}')
