#!/usr/bin/env python3
"""
Fix ZIP codes in all Oklahoma county CSVs.

Common problem: the pipeline extracts ZIP from street address text and grabs the
street number instead of the postal code (e.g. "30626 Aaron Way" → zip=30626).
Fix: if the stored ZIP doesn't look like an Oklahoma ZIP (73xxx or 74xxx),
replace it with the canonical ZIP for that city.

Usage: python3 fix_oklahoma_zips.py [slug ...]
       python3 fix_oklahoma_zips.py          # all OK county CSVs
"""
import csv
import re
import sys
from pathlib import Path

DATA_DIR = Path("app/county-data")

OK_ZIP_RE = re.compile(r'^7[34]\d{3}(-\d{4})?$')

# Canonical ZIPs per city (primary ZIP for the city seat or most common address)
CITY_ZIP = {
    # Oklahoma County
    "oklahoma city": "73102",
    "oklahoma": "73102",       # truncated city name artifact
    "edmond": "73013",
    "moore": "73160",
    "midwest city": "73110",
    "del city": "73115",
    "bethany": "73008",
    "warr acres": "73122",
    "nichols hills": "73116",
    "the village": "73120",
    "choctaw": "73020",
    "harrah": "73045",
    "luther": "73054",
    "jones": "73049",
    "spencer": "73084",
    "arcadia": "73007",
    # Canadian County
    "yukon": "73099",
    "mustang": "73064",
    "el reno": "73036",
    "piedmont": "73078",
    "union city": "73090",
    "calumet": "73014",
    "okarche": "73762",
    "tuttle": "73089",
    "weatherford": "73096",
    # Cleveland County
    "norman": "73069",
    "noble": "73068",
    "lexington": "73051",
    "slaughterville": "73071",
    "goldsby": "73093",
    # Logan County
    "guthrie": "73044",
    "crescent": "73028",
    "cashion": "73016",
    "coyle": "73027",
    "orlando": "73073",
    "marshall": "73056",
    "langston": "73050",
    # Grady County
    "chickasha": "73018",
    "blanchard": "73010",
    "ninnekah": "73067",
    "rush springs": "73082",
    "minco": "73059",
    "amber": "73004",
    "verden": "73092",
    "alex": "73002",
    "bradley": "73011",
    "pocasset": "73075",
    "cement": "73017",
    # McClain County
    "purcell": "73080",
    "newcastle": "73065",
    "lindsay": "73052",
    "washington": "73093",
    "byars": "74831",
    "elmore city": "73433",
    "maysville": "73057",
    # Pottawatomie County
    "shawnee": "74801",
    "tecumseh": "74873",
    "mcloud": "74851",
    "meeker": "74855",
    "prague": "74864",
    "maud": "74854",
    "earlsboro": "74840",
    "bethel acres": "74827",
    # Tulsa County
    "tulsa": "74103",
    "broken arrow": "74012",
    "owasso": "74055",
    "sand springs": "74063",
    "jenks": "74037",
    "bixby": "74008",
    "collinsville": "74021",
    "glenpool": "74033",
    "skiatook": "74070",
    "sperry": "74073",
    "catoosa": "74015",
    # Rogers County
    "claremore": "74017",
    "inola": "74036",
    "foyil": "74031",
    "chelsea": "74016",
    "oologah": "74053",
    "talala": "74083",
    "sequoyah": "74059",
    "verdigris": "74037",
    # Wagoner County
    "wagoner": "74467",
    "coweta": "74429",
    "porter": "74454",
    "okay": "74446",
    "redbird": "74458",
    "tullahassee": "74464",
    # Creek County
    "sapulpa": "74066",
    "bristow": "74010",
    "drumright": "74030",
    "mannford": "74044",
    "kiefer": "74041",
    "depew": "74028",
    "kellyville": "74039",
    "mounds": "74047",
    "oilton": "74052",
    "shamrock": "74068",
    "slick": "74071",
    # Osage County
    "pawhuska": "74056",
    "hominy": "74035",
    "fairfax": "74637",
    "barnsdall": "74002",
    "wynona": "74084",
    "shidler": "74651",
    "avant": "74001",
    "burbank": "74644",
    "osage": "74054",
    # Washington County
    "bartlesville": "74003",
    "dewey": "74029",
    "copan": "74022",
    "ochelata": "74051",
    "ramona": "74061",
    "vera": "74082",
    "south coffeyville": "74072",
    # Okmulgee County
    "okmulgee": "74447",
    "henryetta": "74437",
    "beggs": "74421",
    "dewar": "74431",
    "morris": "74445",
    "schulter": "74460",
    "taft": "74462",
    "preston": "74456",
}


def fix_file(path: Path) -> int:
    rows = list(csv.DictReader(path.open()))
    if not rows:
        return 0
    fieldnames = list(rows[0].keys())
    fixed = 0
    for r in rows:
        z = r.get("zip_code", "").strip()
        if not z or OK_ZIP_RE.match(z):
            continue  # already valid or blank
        city = r.get("city", "").strip().lower()
        new_zip = CITY_ZIP.get(city, "")
        if new_zip:
            r["zip_code"] = new_zip
            fixed += 1
        else:
            r["zip_code"] = ""  # can't fix — blank is better than wrong
            fixed += 1

    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    return fixed


if __name__ == "__main__":
    if sys.argv[1:]:
        paths = [DATA_DIR / f"{s}.csv" for s in sys.argv[1:] if (DATA_DIR / f"{s}.csv").exists()]
    else:
        paths = sorted(DATA_DIR.glob("*-ok.csv"))

    total_fixed = 0
    for p in paths:
        n = fix_file(p)
        if n:
            print(f"  {p.stem}: fixed {n} ZIP(s)")
        total_fixed += n
    print(f"Total ZIPs fixed: {total_fixed}")
