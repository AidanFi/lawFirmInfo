#!/usr/bin/env bash
# Run Oklahoma county pipelines sequentially (after small county tests pass).
# Usage: bash run_ok_pipeline.sh [county_key ...]
# If no args, runs all remaining OK counties.
set -e

COUNTIES=(
    "oklahoma_county"
    "canadian_county"
    "cleveland_county"
    "grady_county"
    "mcclain_county"
    "pottawatomie_county"
    "tulsa_county"
    "rogers_county"
    "wagoner_county"
    "creek_county"
    "okmulgee_county"
)

if [ $# -gt 0 ]; then
    COUNTIES=("$@")
fi

for county in "${COUNTIES[@]}"; do
    echo ""
    echo "======================================================"
    echo "  PIPELINE: $county"
    echo "======================================================"
    python3 -m scraper.county.pipeline --county "$county" --skip-ks-courts 2>&1
    echo "  -> Done: $county"
done
echo ""
echo "All pipelines complete."
