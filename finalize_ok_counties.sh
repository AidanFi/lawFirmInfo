#!/bin/bash
# Post-enrichment finalization for all 14 OK counties
# Run after ok_enrich_websites.py completes
set -e

COUNTIES="canadian cleveland creek grady logan mcclain oklahoma okmulgee osage pottawatomie rogers tulsa wagoner washington"

echo "=== Deep clean pass ==="
for c in $COUNTIES; do
  python3 deep_clean_oklahoma.py ${c}-county-ok 2>&1 | tail -1
done

echo ""
echo "=== ZIP fix pass ==="
for c in $COUNTIES; do
  python3 fix_oklahoma_zips.py ${c}-county-ok 2>&1 | tail -1
done

echo ""
echo "=== Lawyer counts ==="
for c in $COUNTIES; do
  echo "  ${c}-county-ok..."
  python3 add_lawyer_counts.py ${c}-county-ok 2>&1 | tail -2
done

echo ""
echo "=== Update manifest ==="
python3 - <<'PYEOF'
import csv, json
from pathlib import Path

manifest_path = Path('app/county-data/manifest.json')
with open(manifest_path) as f:
    manifest = json.load(f)

data_dir = Path('app/county-data')
for county in manifest['counties']:
    csv_path = data_dir / county['csv_file']
    if csv_path.exists():
        rows = list(csv.DictReader(csv_path.open()))
        county['firm_count'] = len(rows)
        county['last_updated'] = '2026-06-25'

with open(manifest_path, 'w') as f:
    json.dump(manifest, f, indent=2)
print("Manifest updated")
PYEOF

echo ""
echo "=== Final counts ==="
python3 - <<'PYEOF'
import csv
from pathlib import Path
total = 0
for p in sorted(Path('app/county-data').glob('*-ok.csv')):
    rows = list(csv.DictReader(p.open()))
    total += len(rows)
    no_web = sum(1 for r in rows if not r.get('website','').strip())
    print(f"  {p.stem}: {len(rows)} firms, {no_web} without website")
print(f"Total OK: {total}")
PYEOF

echo ""
echo "=== Deploy to gh-pages ==="
git add app/county-data/
git diff --cached --stat

echo ""
echo "Ready to commit and push to gh-pages. Run:"
echo "  git commit -m 'feat: finalize OK county data — enrich websites, clean, lawyer counts'"
echo "  git subtree push --prefix=app gh-pages gh-pages"
