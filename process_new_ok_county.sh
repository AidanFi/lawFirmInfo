#!/usr/bin/env bash
# Run full post-pipeline processing for a new Oklahoma county CSV.
# Usage: bash process_new_ok_county.sh <slug>
# Example: bash process_new_ok_county.sh oklahoma-county-ok
set -e

SLUG="$1"
if [ -z "$SLUG" ]; then
  echo "Usage: bash process_new_ok_county.sh <slug>"
  exit 1
fi

CSV="app/county-data/${SLUG}.csv"
if [ ! -f "$CSV" ]; then
  echo "ERROR: $CSV does not exist — pipeline not complete yet?"
  exit 1
fi

echo "Processing $SLUG..."
echo "  Step 1: clean_county.py"
python3 clean_county.py "$SLUG" 2>&1

echo "  Step 2: deep_clean_oklahoma.py"
python3 deep_clean_oklahoma.py "$SLUG" 2>&1

echo "  Step 3: fix_oklahoma_zips.py"
python3 fix_oklahoma_zips.py "$SLUG" 2>&1

echo "  Step 4: add_lawyer_counts.py"
python3 add_lawyer_counts.py --county "$SLUG" 2>&1

echo ""
echo "Done! Firm count:"
python3 -c "
import csv
from pathlib import Path
rows = list(csv.DictReader(Path('app/county-data/$SLUG.csv').open()))
with_web = sum(1 for r in rows if r.get('website','').strip())
with_phone = sum(1 for r in rows if r.get('phone_number','').strip())
print(f'  {len(rows)} firms | web={with_web} ({with_web*100//len(rows) if rows else 0}%) | phone={with_phone} ({with_phone*100//len(rows) if rows else 0}%)')
"
