#!/usr/bin/env bash
# Add lawyer counts for all Oklahoma county CSVs.
set -e

SLUGS=(
    "oklahoma-county-ok"
    "canadian-county-ok"
    "cleveland-county-ok"
    "logan-county-ok"
    "grady-county-ok"
    "mcclain-county-ok"
    "pottawatomie-county-ok"
    "tulsa-county-ok"
    "rogers-county-ok"
    "wagoner-county-ok"
    "creek-county-ok"
    "osage-county-ok"
    "washington-county-ok"
    "okmulgee-county-ok"
)

for slug in "${SLUGS[@]}"; do
    echo "  Lawyer counts: $slug"
    python3 add_lawyer_counts.py --county "$slug" 2>&1
done
echo "Lawyer counts complete."
