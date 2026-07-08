#!/usr/bin/env python3
"""
Remove confirmed wrong websites and wrong-county records from provider CSVs.
"""
import csv, os, re

DATA_DIR = "app/county-data"

# URLs confirmed wrong: non-healthcare, wrong geography, 404, or physician pages
# assigned to PT/chiro providers via phone sharing
BAD_URLS = {
    # Non-healthcare sites
    "http://karenwrighthomes.com/",
    "http://realestatelawrence.com/",
    "http://haydencatholic.org/",
    "http://dunningexpress.com/",
    # Art museum (KU Spencer Art)
    "https://spencerart.ku.edu/",
    # VA federal hospitals (not private practice)
    "http://www.leavenworth.va.gov/",
    "http://www.topeka.va.gov/",
    # Out-of-state health systems
    "https://account.allinahealth.org/providers/3622",
    "https://cm.childrenswi.org/Physician-Directory/H/Hawley-Nicholas",
    "https://doctors.umiamihealth.org/provider/Patricia+M+Byers/525600",
    # Wrong doctor/NP directory pages assigned via phone sharing
    "https://www.adventhealth.com/find-doctor/doctor/Paul-Moore-MD-1174575930",
    "https://www.adventhealth.com/hospital/adventhealth-shawnee-mission/find-doctor/doctor/john-horton-md-faafp-1861593964",
    "https://www.adventhealth.com/find-doctor/doctor/teresa-self-dnp-aprn-fnp-c-1659159648",
    "https://www.adventhealth.com/find-doctor/doctor/abigail-mae-campbell-pa-c-1417612607",
    "https://www.adventhealth.com/find-doctor/doctor/augustine-v-joseph-md-1750360459",
    "https://www.adventhealth.com/practice/adventhealth-medical-group/adventhealth-medical-group-primary-care-shawnee-mission",
    # Confirmed 404 dead links
    "http://www.wardchiropractic.net/",
    "https://drrockers.com/about-us/",
}

def is_bad_url(url):
    if not url:
        return False
    url = url.strip()
    if url in BAD_URLS:
        return True
    # therapyworks.com — no KS presence, assigned to 12 providers
    if "therapyworks.com" in url:
        return True
    return False

# Wrong-county providers to remove by NPI number + county file
# Format: {csv_filename: [npi_numbers_to_remove]}
WRONG_COUNTY = {
    "providers-johnson-county-ks.csv": [
        "1386186419",  # Emily Rains DC — Lawrence KS → Douglas County
        "1295913317",  # Jon Mccormick DC — Lawrence KS → Douglas County
        "1417791740",  # Amanda Engler PT — 3901 Rainbow Blvd zip 66160 → Wyandotte County
        "1710295761",  # University of Kansas Physicians Inc — 3901 Rainbow Blvd → Wyandotte County
        "1285732461",  # Megan Renee Mcmahan MSPT — Ottawa KS → Franklin County
    ],
}

FIELDNAMES = [
    "provider_name", "website", "phone_number", "provider_type",
    "city", "state", "county", "street_address", "zip_code", "email", "npi_number"
]

def process_file(fname):
    path = os.path.join(DATA_DIR, fname)
    if not os.path.exists(path):
        return

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or FIELDNAMES

    bad_npis = set(WRONG_COUNTY.get(fname, []))
    url_cleared = 0
    rows_removed = 0
    out_rows = []

    for row in rows:
        npi = row.get("npi_number", "").strip()
        if npi in bad_npis:
            rows_removed += 1
            continue
        url = row.get("website", "").strip()
        if is_bad_url(url):
            row["website"] = ""
            url_cleared += 1
        out_rows.append(row)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    changes = []
    if url_cleared:
        changes.append(f"{url_cleared} wrong website(s) cleared")
    if rows_removed:
        changes.append(f"{rows_removed} wrong-county record(s) removed")
    if changes:
        print(f"  {fname}: {', '.join(changes)}")
    else:
        print(f"  {fname}: no changes needed")

def main():
    files = sorted(f for f in os.listdir(DATA_DIR) if f.startswith("providers-") and f.endswith(".csv"))
    total_before = 0
    total_after = 0

    # Quick count before
    for fname in files:
        path = os.path.join(DATA_DIR, fname)
        with open(path, newline="") as f:
            r = list(csv.DictReader(f))
        with_web = sum(1 for row in r if row.get("website","").strip())
        total_before += with_web

    print(f"Provider CSV accuracy cleanup")
    print(f"=" * 50)
    for fname in files:
        process_file(fname)

    # Count after
    for fname in files:
        path = os.path.join(DATA_DIR, fname)
        with open(path, newline="") as f:
            r = list(csv.DictReader(f))
        with_web = sum(1 for row in r if row.get("website","").strip())
        total_after += with_web

    print(f"\nWebsites: {total_before} → {total_after} ({total_after - total_before:+d})")
    print("Done.")

if __name__ == "__main__":
    main()
