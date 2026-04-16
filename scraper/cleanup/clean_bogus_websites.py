#!/usr/bin/env python3
"""Null out existing website entries that match our directory/junk blocklist.

Re-runs scraper.enrich_websites._is_directory_url over every firm's website
and clears the field (plus removes stale source tags) when it matches a
directory/junk domain.

Usage:
    python -m scraper.cleanup.clean_bogus_websites --dry-run   # Report only
    python -m scraper.cleanup.clean_bogus_websites              # Apply
"""
import argparse
import json
import shutil
from datetime import datetime

from scraper.enrich_websites import _is_directory_url

INPUT_PATH = "app/firms_data.js"
BACKUP_PATH = f"/tmp/firms_data_clean_bogus_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.js"


def _load_firms():
    with open(INPUT_PATH) as f:
        content = f.read()
    return json.loads(content[len("const FIRMS_DATA = "):-1])


def _save_firms(data):
    with open(INPUT_PATH, "w") as f:
        f.write("const FIRMS_DATA = ")
        json.dump(data, f, indent=2)
        f.write(";")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    data = _load_firms()
    firms = data["firms"]

    bogus = []
    for firm in firms:
        url = firm.get("website")
        if url and _is_directory_url(url):
            bogus.append(firm)

    print(f"[clean-bogus] Found {len(bogus)} firms with directory/junk URLs")
    for f in bogus[:25]:
        print(f"  {f['name'][:45]:<45} | {f['website']}")
    if len(bogus) > 25:
        print(f"  ... and {len(bogus) - 25} more")

    if args.dry_run or not bogus:
        return

    shutil.copy(INPUT_PATH, BACKUP_PATH)
    print(f"[clean-bogus] Backed up to {BACKUP_PATH}")

    # Null out website; no generic "remove source tag" because different sources
    # might have contributed — just clear the URL so it can be re-enriched.
    for firm in bogus:
        firm["website"] = None

    data["meta"]["lastCleaned"] = datetime.now().isoformat()
    _save_firms(data)

    print(f"[clean-bogus] Nulled {len(bogus)} website URLs")


if __name__ == "__main__":
    main()
