#!/usr/bin/env bash
# Run clean_county.py for all Oklahoma county slugs.
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
    echo ""
    echo "======================================================"
    echo "  CLEAN: $slug"
    echo "======================================================"
    python3 clean_county.py "$slug" 2>&1
done
echo ""
echo "All clean passes complete."
