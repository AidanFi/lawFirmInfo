#!/usr/bin/env python3
"""Audit firms for Kansas City, MO contamination and remove clear non-KS entries.

All firms are currently stamped state=KS regardless of truth. The Avvo scraper
in particular pulled ~1,982 "Kansas City" attorneys, many of whom practice in
KC, MO. This script identifies firms with positive evidence of being in Missouri
and removes them.

Signals (a firm is flagged as MO if ANY of):
- Phone area code is MO-only (816, 660, 573, 417, 314, 636)
- ZIP starts with a MO prefix (64xxx, 65xxx)
- Street address contains ", MO" or " Missouri"
- Coordinates fall east of -94.6° and north of 38.8° (KCMO bounding box)

Conservative: firms with no phone, no zip, no coords, no address markers are
KEPT. We only remove firms with explicit MO evidence.

Usage:
    python -m scraper.cleanup.kc_mo_audit              # Audit + remove
    python -m scraper.cleanup.kc_mo_audit --dry-run     # Report only, no changes
"""
import argparse
import json
import re
import shutil
from datetime import datetime

INPUT_PATH = "app/firms_data.js"
BACKUP_PATH = f"/tmp/firms_data_kc_mo_audit_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.js"

# Missouri-only area codes (ignore overlap: 913 is KS-only, 816 is MO-only)
_MO_AREA_CODES = {"816", "660", "573", "417", "314", "636", "557"}
# Kansas area codes (for sanity checks)
_KS_AREA_CODES = {"913", "785", "620", "316"}

# MO ZIP prefixes: 63xxx (St. Louis area), 64xxx (KCMO, west MO), 65xxx (Springfield, central MO)
_MO_ZIP_PREFIXES = re.compile(r"^(63|64|65)\d{3}")
# KS ZIP prefixes: 66xxx, 67xxx
_KS_ZIP_PREFIXES = re.compile(r"^(66|67)\d{3}")

# Address patterns for explicit Missouri
_MO_ADDRESS = re.compile(
    r"(?:,\s*MO\b|,\s*Missouri\b|\bMissouri\s+\d{5}\b|\bKansas\s+City,?\s*MO\b)",
    re.IGNORECASE,
)


def _extract_area_code(phone):
    """Extract US area code from a phone string. Returns None if not found."""
    if not phone:
        return None
    digits = re.sub(r"\D", "", phone)
    if len(digits) == 10:
        return digits[:3]
    if len(digits) == 11 and digits.startswith("1"):
        return digits[1:4]
    return None


# Kansas City metro area (KS side). Used to decide whether an 816 phone is meaningful.
_KC_METRO_KS = {
    "Kansas City", "Leawood", "Lenexa", "Overland Park", "Shawnee",
    "Olathe", "Prairie Village", "Mission", "Merriam", "Fairway",
    "Westwood", "Roeland Park", "Gardner", "Edgerton", "Spring Hill",
    "Bonner Springs", "Basehor", "Tonganoxie", "Lake Quivira",
    "Mission Hills", "De Soto", "Mission Woods", "Westwood Hills",
    "Countryside", "Lansing",
}


def _classify_firm(firm):
    """Classify a firm as 'MO', 'KS', or 'UNKNOWN' based on available signals.

    Flag as MO only on STRONG, unambiguous evidence:
    - MO ZIP prefix (63/64/65)                      — zip itself locates the office
    - Explicit ", MO" / "Missouri" in the address   — textual confirmation
    - Coordinates inside KCMO bounding box           — geocoded outside KS

    Phone area code is intentionally NOT used as a flag on its own: many
    Kansas firms (especially in Johnson County) use 816 numbers legitimately
    because the metro shares both area codes. Removing on phone alone would
    delete real KS firms.

    Returns (classification, reasons).
    """
    reasons = []
    addr = firm.get("address") or {}
    street = addr.get("street") or ""
    city = addr.get("city") or ""
    zip_code = addr.get("zip") or ""
    coords = firm.get("coordinates") or {}

    # STRONG: ZIP code locates the office in MO
    if zip_code and _MO_ZIP_PREFIXES.match(zip_code):
        reasons.append(f"zip={zip_code}")
        return "MO", reasons

    # STRONG: explicit MO address marker
    full_addr = f"{street} {city}"
    if _MO_ADDRESS.search(full_addr):
        reasons.append("address_has_MO")
        return "MO", reasons

    # STRONG: coordinates firmly in KCMO
    lat = coords.get("lat")
    lng = coords.get("lng")
    if lat is not None and lng is not None:
        # KCMO core: east of state-line (-94.6), in KC metro lat range
        if 38.8 <= lat <= 39.5 and -94.6 <= lng <= -94.2:
            reasons.append(f"coords_KCMO=({lat:.3f},{lng:.3f})")
            return "MO", reasons
        # Clearly east of KS entirely (excluding deep south/north)
        if lng > -94.6 and 36.5 <= lat <= 40.6:
            reasons.append(f"coords_east=({lat:.3f},{lng:.3f})")
            return "MO", reasons

    # KS-positive: has a KS ZIP
    if zip_code and _KS_ZIP_PREFIXES.match(zip_code):
        return "KS", []

    return "UNKNOWN", []


def _load_firms():
    with open(INPUT_PATH) as f:
        content = f.read()
    json_str = content[len("const FIRMS_DATA = "):-1]
    return json.loads(json_str)


def _save_firms(data):
    with open(INPUT_PATH, "w") as f:
        f.write("const FIRMS_DATA = ")
        json.dump(data, f, indent=2)
        f.write(";")


def main():
    parser = argparse.ArgumentParser(description="Audit firms for MO contamination")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report findings without modifying data")
    args = parser.parse_args()

    data = _load_firms()
    firms = data["firms"]
    total = len(firms)

    print(f"[kc-mo-audit] Analyzing {total} firms...")

    mo_firms = []
    ks_firms = []
    unknown_firms = []

    for firm in firms:
        cls, reasons = _classify_firm(firm)
        if cls == "MO":
            mo_firms.append((firm, reasons))
        elif cls == "KS":
            ks_firms.append(firm)
        else:
            unknown_firms.append(firm)

    print()
    print(f"Classification results:")
    print(f"  KS (positive):    {len(ks_firms):>5}")
    print(f"  MO (positive):    {len(mo_firms):>5}")
    print(f"  UNKNOWN:          {len(unknown_firms):>5}")
    print(f"  Total:            {total:>5}")
    print()

    if mo_firms:
        print("Sample 20 MO-flagged firms (will be removed):")
        for firm, reasons in mo_firms[:20]:
            a = firm.get("address") or {}
            print(f"  {firm['name'][:40]:<40} | {a.get('city'):<15} | {firm.get('phone') or '':<15} | {','.join(reasons)}")
        print()

    if args.dry_run:
        print("[kc-mo-audit] DRY RUN — no changes made")
        return

    if not mo_firms:
        print("[kc-mo-audit] No MO-flagged firms found. Nothing to do.")
        return

    # Back up
    shutil.copy(INPUT_PATH, BACKUP_PATH)
    print(f"[kc-mo-audit] Backed up current data to {BACKUP_PATH}")

    # Build new firm list with MO entries removed
    mo_ids = {id(f) for f, _ in mo_firms}
    new_firms = [f for f in firms if id(f) not in mo_ids]

    removed_with_websites = sum(1 for f, _ in mo_firms if f.get("website"))

    data["firms"] = new_firms
    data["meta"]["lastCleaned"] = datetime.now().isoformat()
    _save_firms(data)

    print()
    print(f"[kc-mo-audit] Removed {len(mo_firms)} MO firms")
    print(f"[kc-mo-audit]   ({removed_with_websites} had websites)")
    print(f"[kc-mo-audit] New total: {len(new_firms)} firms")


if __name__ == "__main__":
    main()
