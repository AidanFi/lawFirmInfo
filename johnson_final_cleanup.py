import csv
import json
import os

CSV_PATH = "/Users/aidanfields/lawFirmInfo/app/county-data/johnson-county-ks.csv"
MANIFEST_PATH = "/Users/aidanfields/lawFirmInfo/app/county-data/manifest.json"

REMOVALS = {
    "mr. gyros greek food & pastries",
    "adventhealth",
    "adventhealth shawnee mission",
    "kvc health systems inc",
    "aon",
    "metlife",
    "cna",
    "gitlab inc",
    "trueml technologies llc",
    "investcloud, inc.",
    "avi-spl llc",
    "robert w. baird & co. inc.",
    "marksnelson llc",
    "creativeone wealth, llc",
    "kansas gas service, a division of one gas, inc.",
    "e g energy, llc",
    "hill's pet nutrition",
    "hill's pet nutrition inc",
    "smith & loveless",
    "smith & loveless inc",
    "spx cooling tech, llc",
    "qc holdings, llc",
    "airshare",
    "section 4 strategies",
    "adastra strategies llc",
    "neu consulting group, llc dba neuanalytics, llc",
    "azimuthzero, llc",
    "brr architecture, inc",
    "dlr group",
    "dlr group inc.",
    "dentists of lenexa",
    "tip n ring communications inc",
    "reese & nichols",
    "reece & nichols",
    "keller williams",
    "wedolocal.com",
    "fanthreesixty",
    "seaboard foods",
    "state of kansas",
    "personal",
    "n/a",
    "na",
    "ulah",
    "for safe keeping inc",
    "logs group",
    "attorney overland park",
    "attorney white collar crime",
    "top rated lawyer 10/10",
    "top rated lawyer 9.5/10",
    "spring hill, ks lawyer with",
    "overland park, ks lawyer with",
    "olathe, ks lawyer with",
    "lenexa, ks lawyer with",
    "shawnee, ks lawyer with",
    "westwood, ks lawyer with",
    "merriam, ks lawyer with",
    "olathe, ks lawyer",
    "overland park, ks lawyer",
    "leawood, ks lawyer",
    "shawnee, ks lawyer",
    "lenexa, ks lawyer",
    "peter",
    "roy",
    "trey",
    "winbigler",
    "cameron",
    "ellis",
    "poirier",
    "griffin",
    "frank",
    "john",
    "neal",
    "estate & trust law - top lawyers usa",
    "faces and places of toledo law",
    "yw",
    "mckinneysusan",
    "uhlig",
}

NAME_FIXES = {
    "attorney - the demps law firm": "The Demps Law Firm",
    "what we offer - foley & foley, pc": "Foley & Foley, PC",
    "criminal practice - daniel ross, llc": "Daniel Ross, LLC",
    "attorney profile - maples & fontenot llp": "Maples & Fontenot LLP",
    "lawyer profiles - johnston law firm": "Johnston Law Firm",
}

def main():
    # Read CSV
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    before_count = len(rows)

    kept = []
    removed = []
    renamed = []

    for row in rows:
        name = row.get("law_firm_name", "")
        name_lower = name.strip().lower()

        if name_lower in REMOVALS:
            removed.append(name)
            continue

        if name_lower in NAME_FIXES:
            new_name = NAME_FIXES[name_lower]
            renamed.append((name, new_name))
            row["law_firm_name"] = new_name

        kept.append(row)

    after_count = len(kept)

    # Write CSV back
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        # detect line ending
        pass

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(kept)

    # Update manifest
    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    old_total = manifest.get("total_firms", 0)
    old_johnson_count = None

    for county in manifest["counties"]:
        if county["slug"] == "johnson-county-ks":
            old_johnson_count = county["firm_count"]
            county["firm_count"] = after_count
            break

    diff = after_count - (old_johnson_count or before_count)
    manifest["total_firms"] = old_total + diff

    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")

    print(f"Before: {before_count} firms")
    print(f"After:  {after_count} firms")
    print(f"Removed: {before_count - after_count} firms")
    if removed:
        print(f"\nRemoved firms ({len(removed)}):")
        for r in removed:
            print(f"  - {r}")
    if renamed:
        print(f"\nRenamed firms ({len(renamed)}):")
        for old, new in renamed:
            print(f"  {old!r} -> {new!r}")
    print(f"\nManifest updated: johnson-county-ks firm_count {old_johnson_count} -> {after_count}")
    print(f"Manifest total_firms: {old_total} -> {manifest['total_firms']}")

if __name__ == "__main__":
    main()
