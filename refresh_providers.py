#!/usr/bin/env python3
"""
Comprehensive provider data refresh using zip code queries.
- Queries NPI by postal_code for ALL confirmed county zip codes
- Merges new providers with existing data (dedup by NPI)
- Removes providers whose zip codes clearly belong to other counties
"""
import csv, re, time, requests
from pathlib import Path

DATA_DIR = Path("app/county-data")
NPI_URL = "https://npiregistry.cms.hhs.gov/api/"

FIELDNAMES = [
    "provider_name", "website", "phone_number", "provider_type",
    "city", "state", "county", "street_address", "zip_code", "email", "npi_number"
]

# Definitive zip codes per county (only confirmed-correct zips)
COUNTY_ZIPS = {
    "johnson-county-ks": {
        "66030","66051","66061","66062","66063","66083","66085",
        "66202","66203","66204","66205","66206","66207","66208",
        "66209","66210","66211","66212","66213","66214","66215",
        "66216","66217","66218","66219","66220","66221","66222",
        "66223","66224","66225","66226","66227","66251",
    },
    "wyandotte-county-ks": {
        "66012","66101","66102","66103","66104","66105","66106",
        "66109","66111","66112","66113","66115","66118","66119","66160",
    },
    "leavenworth-county-ks": {
        "66007","66020","66043","66048","66052","66086",
    },
    "miami-county-ks": {
        "66026","66053","66064","66071",
    },
    "linn-county-ks": {
        "66040","66058","66075","66761",
    },
    "douglas-county-ks": {
        "66006","66025","66044","66045","66046","66047","66049",
    },
    "franklin-county-ks": {
        "66067","66092","66095",
    },
    "jefferson-county-ks": {
        "66054","66066","66073","66087","66088","66097","66512",
    },
    "osage-county-ks": {
        "66413","66451","66523","66524","66537",
    },
    "shawnee-county-ks": {
        "66533","66542","66603","66604","66605","66606","66607",
        "66608","66609","66610","66611","66612","66613","66614",
        "66615","66616","66617","66618","66619","66621","66622","66636",
    },
}

PROVIDER_TYPES = {
    "chiropractor": "Chiropractor",
    "physical therapist": "Physical Therapist",
}

COUNTY_NAMES = {
    "johnson-county-ks": "Johnson County",
    "wyandotte-county-ks": "Wyandotte County",
    "leavenworth-county-ks": "Leavenworth County",
    "miami-county-ks": "Miami County",
    "linn-county-ks": "Linn County",
    "douglas-county-ks": "Douglas County",
    "franklin-county-ks": "Franklin County",
    "jefferson-county-ks": "Jefferson County",
    "osage-county-ks": "Osage County",
    "shawnee-county-ks": "Shawnee County",
}

def clean_phone(raw):
    digits = re.sub(r"[^\d]","",raw or "")
    if len(digits)==10: return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    if len(digits)==11 and digits[0]=="1": return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return raw or ""

def clean_zip(raw):
    digits = re.sub(r"[^\d]","",raw or "")
    return digits[:5] if len(digits)>=5 else digits

def extract_record(result, provider_type, county_name):
    basic = result.get("basic",{})
    addresses = result.get("addresses",[])
    enum_type = result.get("enumeration_type","")
    if enum_type=="NPI-2":
        name = basic.get("organization_name","").strip().title()
    else:
        first = basic.get("first_name","").strip()
        middle = basic.get("middle_name","").strip()
        last = basic.get("last_name","").strip()
        cred = basic.get("credential","").strip()
        parts = [p for p in [first,middle,last] if p]
        name = " ".join(parts).title()
        if cred and cred!="--": name = f"{name}, {cred}"
    addr = next((a for a in addresses if a.get("address_purpose")=="LOCATION"),
                addresses[0] if addresses else {})
    street = addr.get("address_1","").strip().title()
    if addr.get("address_2"): street = f"{street} {addr['address_2'].strip().title()}".strip()
    return {
        "provider_name": name,
        "website": "",
        "phone_number": clean_phone(addr.get("telephone_number","")),
        "provider_type": provider_type,
        "city": addr.get("city","").strip().title(),
        "state": addr.get("state","").strip().upper(),
        "county": county_name,
        "street_address": street,
        "zip_code": clean_zip(addr.get("postal_code","")),
        "email": "",
        "npi_number": result.get("number",""),
    }

def fetch_npi_zip(taxonomy, state, postal_code):
    results=[]
    skip=0
    while True:
        params={"version":"2.1","taxonomy_description":taxonomy,"state":state,
                "postal_code":postal_code,"limit":200,"skip":skip}
        try:
            r=requests.get(NPI_URL,params=params,timeout=20)
            data=r.json()
        except Exception as e:
            print(f"      NPI error: {e}"); break
        if data.get("Errors"): break
        batch=data.get("results",[])
        if not batch: break
        results.extend(batch)
        if len(batch)<200: break
        skip+=200
        time.sleep(0.3)
    return results

def process_county(slug):
    county_name = COUNTY_NAMES[slug]
    valid_zips = COUNTY_ZIPS[slug]
    csv_path = DATA_DIR / f"providers-{slug}.csv"

    # Load existing data
    existing = {}
    if csv_path.exists():
        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                npi = row.get("npi_number","").strip()
                if npi: existing[npi] = row

    before_count = len(existing)

    # Step 1: remove wrong-county providers (zip not in valid set)
    wrong_zip = {npi: r for npi,r in existing.items()
                 if r.get("zip_code","").strip()[:5] not in valid_zips}
    if wrong_zip:
        print(f"  Removing {len(wrong_zip)} wrong-county providers:")
        for npi,r in sorted(wrong_zip.items(), key=lambda x: x[1].get("zip_code","")):
            print(f"    {r['provider_name']} — {r['city']}, zip {r['zip_code']}")
        for npi in wrong_zip:
            del existing[npi]

    # Step 2: query NPI by postal_code for every confirmed zip
    added = 0
    for zip_code in sorted(valid_zips):
        for taxonomy, display_type in PROVIDER_TYPES.items():
            raw = fetch_npi_zip(taxonomy, "KS", zip_code)
            for res in raw:
                rec = extract_record(res, display_type, county_name)
                npi = rec["npi_number"]
                if not npi or npi in existing: continue
                if rec.get("state","") not in ("KS",""): continue
                # Only add if zip confirms this county
                if rec.get("zip_code","").strip()[:5] not in valid_zips: continue
                existing[npi] = rec
                added += 1
            time.sleep(0.15)

    # Step 3: preserve existing websites for carried-over providers
    rows = list(existing.values())
    rows.sort(key=lambda r: (r.get("provider_type",""), r.get("city",""), r.get("provider_name","")))

    with open(csv_path,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(rows)

    chiro = sum(1 for r in rows if r.get("provider_type")=="Chiropractor")
    pt = sum(1 for r in rows if r.get("provider_type")=="Physical Therapist")
    with_web = sum(1 for r in rows if r.get("website","").strip())
    removed = before_count - (len(rows) - added)
    print(f"  Result: {len(rows)} providers (was {before_count}, -{removed} wrong-county, +{added} new)")
    print(f"  Chiro: {chiro} | PT: {pt} | With website: {with_web}")
    return len(rows)

def main():
    print("Provider data refresh — zip-code based\n")
    total = 0
    for slug in COUNTY_ZIPS:
        print(f"\n{'='*55}")
        print(f"{COUNTY_NAMES[slug]}")
        print(f"{'='*55}")
        total += process_county(slug)
    print(f"\n\nTotal providers across all counties: {total}")

if __name__=="__main__":
    main()
