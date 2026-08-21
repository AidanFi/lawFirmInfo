#!/usr/bin/env python3
"""
KS counties quality cleanup pass:
- Remove keyword dumps (Top Rated Lawyer X/10, City KS Attorney with, etc.)
- Remove single first-names with no website/source
- Update manifest
"""
import csv, re, json
from pathlib import Path

DATA_DIR = Path('/Users/aidanfields/lawFirmInfo/app/county-data')
MANIFEST_PATH = DATA_DIR / 'manifest.json'

COUNTIES = [
    'wyandotte-county-ks',
    'leavenworth-county-ks',
    'miami-county-ks',
    'linn-county-ks',
]

# Keyword dump patterns
KW_DUMP = re.compile(
    r'(^top\s+rated\s+(lawyer|attorney)\s+\d+(\.\d+)?/10)'
    r'|(^.{3,50},\s*ks\s+(attorney|lawyer|law\s+firm)\s+with\s*$)'
    r'|(^(attorney|lawyer)\s+[a-z]+$)',  # "Attorney Zimmerman" etc.
    re.IGNORECASE
)

# Single first-name only entries (single word, no law indicator, no website, no source)
LAW_IND = re.compile(
    r'\b(law|legal|attorney|attorneys|counsel|llp|pllc|p\.c\.|p\.a\.|esq|firm|mediator|arbitr|litigat|paralegal)\b',
    re.I
)

SINGLE_FIRST_NAMES = {
    'ronald', 'john', 'james', 'mary', 'david', 'michael', 'robert', 'william',
    'richard', 'thomas', 'charles', 'gary', 'mark', 'donald', 'kenneth',
    'steven', 'george', 'edward', 'brian', 'larry', 'jeffrey', 'frank', 'scott',
    'eric', 'stephen', 'paul', 'andrew', 'kevin', 'joshua', 'raymond',
    'gregory', 'jerry', 'dennis', 'walter', 'patrick', 'peter', 'harold',
    'douglas', 'henry', 'carl', 'arthur', 'ryan', 'roger', 'joe', 'juan',
    'jack', 'albert', 'jonathan', 'justin', 'terry', 'gerald', 'keith',
    'samuel', 'willie', 'ralph', 'lawrence', 'nicholas', 'roy', 'benjamin',
    'bruce', 'brandon', 'adam', 'harry', 'fred', 'wayne', 'billy', 'steve',
    'louis', 'jeremy', 'aaron', 'randy', 'howard', 'eugene', 'carlos', 'russell',
    'bobby', 'victor', 'martin', 'ernest', 'phillip', 'todd', 'jesse', 'craig',
    'alan', 'shawn', 'clarence', 'sean', 'philip', 'chris', 'johnny', 'earl',
    'jimmy', 'antonio', 'danny', 'bryan', 'tony', 'luis', 'mike', 'stanley',
    'leonard', 'nathan', 'dale', 'manuel', 'rodney', 'curtis', 'norman',
    'allen', 'marvin', 'vincent', 'glenn', 'jeffery', 'travis', 'jeff',
    'chad', 'jacob', 'lee', 'melvin', 'alfred', 'kyle', 'francis', 'bradley',
    'tim', 'ron', 'tommy', 'dan', 'trey', 'cam', 'cameron', 'ellis', 'griffin',
    'winbigler', 'poirier', 'neal', 'ulah',
}

results = {}

for slug in COUNTIES:
    path = DATA_DIR / f'{slug}.csv'
    rows = list(csv.DictReader(open(path)))
    fieldnames = list(rows[0].keys()) if rows else []
    original = len(rows)
    removed = []

    kept = []
    for r in rows:
        name = r.get('law_firm_name', '').strip()
        name_lc = name.lower()
        src = r.get('sources', '').strip()
        web = r.get('website', '').strip()

        if KW_DUMP.search(name):
            removed.append(f'KW: {name!r}')
            continue

        # Single-word first-name only with no website and no source
        if (len(name.split()) == 1
                and name_lc in SINGLE_FIRST_NAMES
                and not web and not src
                and not LAW_IND.search(name)):
            removed.append(f'FIRST-NAME: {name!r}')
            continue

        kept.append(r)

    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(kept)

    results[slug] = len(kept)
    print(f'{slug}: {original} → {len(kept)} (removed {len(removed)})')
    for note in removed:
        print(f'  - {note}')

# Update manifest
manifest = json.loads(MANIFEST_PATH.read_text())
for county in manifest['counties']:
    if county['slug'] in results:
        county['firm_count'] = results[county['slug']]

total = sum(c['firm_count'] for c in manifest['counties'])
manifest['total_firms'] = total
MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))
print(f'\nManifest total: {total}')
