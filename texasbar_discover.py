#!/usr/bin/env python3
"""
State Bar of Texas Member Directory discovery — county-level.

The State Bar's public "Find a Lawyer" directory (texasbar.com) is a public
service under Texas Government Code Section 81.115 and is searchable by
county. It is the Texas equivalent of the KS Courts registry: the true
completeness ceiling of licensed attorneys with a primary practice address
in a given county.

Search is a stateless POST (no session/cookies required) to:
  Result_form_client.cfm  with fields: Submitted=1, Find=1, County=<id>, Start=<n>
25 results per page. Each card gives: attorney name, firm/company (h5, often
blank), street/city/state/zip, phone (tel: link), status icon, ContactID.

Status icons (legend on the search-results page):
  green circle  = Eligible to practice   <- ONLY status we keep
  red square    = Not Eligible to practice
  yellow club   = Non-Practicing
  aqua diamond  = Inactive
  blue triangle = Deceased

Usage: python3 texasbar_discover.py <county-slug>
"""
import csv
import json
import re
import sys
import time
from pathlib import Path

import requests

DATA_DIR = Path("app/county-data")
CACHE_DIR = Path("data/county")

SEARCH_URL = (
    "https://www.texasbar.com/AM/Template.cfm?Section=Find_A_Lawyer"
    "&Template=/CustomSource/MemberDirectory/Result_form_client.cfm"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}

# Authoritative id for every one of the 254 Texas counties (+ Out of State /
# Out of Country), fetched directly from the site's real county <select>
# element (Section=Find_A_Lawyer&Template=/CustomSource/MemberDirectory/
# Search_Form_Client_Main.cfm — NOT the Search_form_client.cfm template,
# which renders a page with no county dropdown at all and cost real time
# to rule out). This ALSO happens to equal each county's plain alphabetical
# rank among the 254 (verified against every id added by hand before this
# was found: Bexar=15, Collin=43, Dallas=57, Denton=61, Fort Bend=79,
# Harris=101, Tarrant=220, Travis=227 all match) — but don't re-derive by
# hand-counting the alphabet for a new county; look it up here instead,
# it's exact and there's no risk of a long-list counting slip
# (caught one hand-counting itself while adding Montgomery: 171 by hand
# vs. the real 170). Harris also has a genuine SECOND dropdown entry at
# id 102 in the live rendered page (a site data-entry duplicate — this
# canonical list shows 102 as HARRISON, a different real county, so the
# two aren't reconcilable; both must be queried for Harris specifically,
# already handled in TEXAS_COUNTIES below).
ALL_TEXAS_COUNTY_IDS = {
    "ANDERSON": 1, "ANDREWS": 2, "ANGELINA": 3, "ARANSAS": 4, "ARCHER": 5,
    "ARMSTRONG": 6, "ATASCOSA": 7, "AUSTIN": 8, "BAILEY": 9, "BANDERA": 10,
    "BASTROP": 11, "BAYLOR": 12, "BEE": 13, "BELL": 14, "BEXAR": 15,
    "BLANCO": 16, "BORDEN": 17, "BOSQUE": 18, "BOWIE": 19, "BRAZORIA": 20,
    "BRAZOS": 21, "BREWSTER": 22, "BRISCOE": 23, "BROOKS": 24, "BROWN": 25,
    "BURLESON": 26, "BURNET": 27, "CALDWELL": 28, "CALHOUN": 29,
    "CALLAHAN": 30, "CAMERON": 31, "CAMP": 32, "CARSON": 33, "CASS": 34,
    "CASTRO": 35, "CHAMBERS": 36, "CHEROKEE": 37, "CHILDRESS": 38,
    "CLAY": 39, "COCHRAN": 40, "COKE": 41, "COLEMAN": 42, "COLLIN": 43,
    "COLLINGSWORTH": 44, "COLORADO": 45, "COMAL": 46, "COMANCHE": 47,
    "CONCHO": 48, "COOKE": 49, "CORYELL": 50, "COTTLE": 51, "CRANE": 52,
    "CROCKETT": 53, "CROSBY": 54, "CULBERSON": 55, "DALLAM": 56,
    "DALLAS": 57, "DAWSON": 58, "DEAF SMITH": 59, "DELTA": 60,
    "DENTON": 61, "DEWITT": 62, "DICKENS": 63, "DIMMIT": 64, "DONLEY": 65,
    "DUVAL": 66, "EASTLAND": 67, "ECTOR": 68, "EDWARDS": 69, "ELLIS": 70,
    "EL PASO": 71, "ERATH": 72, "FALLS": 73, "FANNIN": 74, "FAYETTE": 75,
    "FISHER": 76, "FLOYD": 77, "FOARD": 78, "FORT BEND": 79,
    "FRANKLIN": 80, "FREESTONE": 81, "FRIO": 82, "GAINES": 83,
    "GALVESTON": 84, "GARZA": 85, "GILLESPIE": 86, "GLASSCOCK": 87,
    "GOLIAD": 88, "GONZALES": 89, "GRAY": 90, "GRAYSON": 91, "GREGG": 92,
    "GRIMES": 93, "GUADALUPE": 94, "HALE": 95, "HALL": 96, "HAMILTON": 97,
    "HANSFORD": 98, "HARDEMAN": 99, "HARDIN": 100, "HARRIS": 101,
    "HARRISON": 102, "HARTLEY": 103, "HASKELL": 104, "HAYS": 105,
    "HEMPHILL": 106, "HENDERSON": 107, "HIDALGO": 108, "HILL": 109,
    "HOCKLEY": 110, "HOOD": 111, "HOPKINS": 112, "HOUSTON": 113,
    "HOWARD": 114, "HUDSPETH": 115, "HUNT": 116, "HUTCHINSON": 117,
    "IRION": 118, "JACK": 119, "JACKSON": 120, "JASPER": 121,
    "JEFF DAVIS": 122, "JEFFERSON": 123, "JIM HOGG": 124,
    "JIM WELLS": 125, "JOHNSON": 126, "JONES": 127, "KARNES": 128,
    "KAUFMAN": 129, "KENDALL": 130, "KENEDY": 131, "KENT": 132,
    "KERR": 133, "KIMBLE": 134, "KING": 135, "KINNEY": 136,
    "KLEBERG": 137, "KNOX": 138, "LAMAR": 139, "LAMB": 140,
    "LAMPASAS": 141, "LASALLE": 142, "LAVACA": 143, "LEE": 144,
    "LEON": 145, "LIBERTY": 146, "LIMESTONE": 147, "LIPSCOMB": 148,
    "LIVE OAK": 149, "LLANO": 150, "LOVING": 151, "LUBBOCK": 152,
    "LYNN": 153, "MCCULLOCH": 154, "MCLENNAN": 155, "MCMULLEN": 156,
    "MADISON": 157, "MARION": 158, "MARTIN": 159, "MASON": 160,
    "MATAGORDA": 161, "MAVERICK": 162, "MEDINA": 163, "MENARD": 164,
    "MIDLAND": 165, "MILAM": 166, "MILLS": 167, "MITCHELL": 168,
    "MONTAGUE": 169, "MONTGOMERY": 170, "MOORE": 171, "MORRIS": 172,
    "MOTLEY": 173, "NACOGDOCHES": 174, "NAVARRO": 175, "NEWTON": 176,
    "NOLAN": 177, "NUECES": 178, "OCHILTREE": 179, "OLDHAM": 180,
    "ORANGE": 181, "PALO PINTO": 182, "PANOLA": 183, "PARKER": 184,
    "PARMER": 185, "PECOS": 186, "POLK": 187, "POTTER": 188,
    "PRESIDIO": 189, "RAINS": 190, "RANDALL": 191, "REAGAN": 192,
    "REAL": 193, "RED RIVER": 194, "REEVES": 195, "REFUGIO": 196,
    "ROBERTS": 197, "ROBERTSON": 198, "ROCKWALL": 199, "RUNNELS": 200,
    "RUSK": 201, "SABINE": 202, "SAN AUGUSTINE": 203, "SAN JACINTO": 204,
    "SAN PATRICIO": 205, "SAN SABA": 206, "SCHLEICHER": 207,
    "SCURRY": 208, "SHACKELFORD": 209, "SHELBY": 210, "SHERMAN": 211,
    "SMITH": 212, "SOMERVELL": 213, "STARR": 214, "STEPHENS": 215,
    "STERLING": 216, "STONEWALL": 217, "SUTTON": 218, "SWISHER": 219,
    "TARRANT": 220, "TAYLOR": 221, "TERRELL": 222, "TERRY": 223,
    "THROCKMORTON": 224, "TITUS": 225, "TOM GREEN": 226, "TRAVIS": 227,
    "TRINITY": 228, "TYLER": 229, "UPSHUR": 230, "UPTON": 231,
    "UVALDE": 232, "VAL VERDE": 233, "VAN ZANDT": 234, "VICTORIA": 235,
    "WALKER": 236, "WALLER": 237, "WARD": 238, "WASHINGTON": 239,
    "WEBB": 240, "WHARTON": 241, "WHEELER": 242, "WICHITA": 243,
    "WILBARGER": 244, "WILLACY": 245, "WILLIAMSON": 246, "WILSON": 247,
    "WINKLER": 248, "WISE": 249, "WOOD": 250, "YOAKUM": 251,
    "YOUNG": 252, "ZAPATA": 253, "ZAVALA": 254,
    "OUT OF STATE": 255, "OUT OF COUNTRY": 256,
}

# County name -> list of "County" select option ids on the search form.
# Some counties (observed: HARRIS) appear twice in the State Bar's dropdown
# under two different ids — both must be queried to get full coverage.
TEXAS_COUNTIES = {
    "harris-county-tx": {
        "name": "Harris",
        "state": "TX",
        "msa": "Houston",
        "county_ids": [101, 102],
    },
    "dallas-county-tx": {
        "name": "Dallas",
        "state": "TX",
        "msa": "Dallas-Fort Worth",
        "county_ids": [57],
    },
    "tarrant-county-tx": {
        "name": "Tarrant",
        "state": "TX",
        "msa": "Dallas-Fort Worth",
        "county_ids": [220],
    },
    "bexar-county-tx": {
        "name": "Bexar",
        "state": "TX",
        "msa": "San Antonio",
        "county_ids": [15],
    },
    "travis-county-tx": {
        "name": "Travis",
        "state": "TX",
        "msa": "Austin",
        "county_ids": [227],
    },
    "collin-county-tx": {
        "name": "Collin",
        "state": "TX",
        "msa": "Dallas-Fort Worth",
        "county_ids": [43],
    },
    "denton-county-tx": {
        "name": "Denton",
        "state": "TX",
        "msa": "Dallas-Fort Worth",
        "county_ids": [61],
    },
    "fort-bend-county-tx": {
        "name": "Fort Bend",
        "state": "TX",
        "msa": "Houston",
        "county_ids": [79],
    },
    "hidalgo-county-tx": {
        "name": "Hidalgo",
        "state": "TX",
        "msa": "McAllen-Edinburg-Mission",
        "county_ids": [108],
    },
    "el-paso-county-tx": {
        "name": "El Paso",
        "state": "TX",
        "msa": "El Paso",
        "county_ids": [71],
    },
    "montgomery-county-tx": {
        "name": "Montgomery",
        "state": "TX",
        "msa": "Houston",
        "county_ids": [170],
    },
    "williamson-county-tx": {
        "name": "Williamson",
        "state": "TX",
        "msa": "Austin",
        "county_ids": [ALL_TEXAS_COUNTY_IDS["WILLIAMSON"]],
    },
    "cameron-county-tx": {
        "name": "Cameron",
        "state": "TX",
        "msa": "Brownsville-Harlingen",
        "county_ids": [ALL_TEXAS_COUNTY_IDS["CAMERON"]],
    },
    "brazoria-county-tx": {
        "name": "Brazoria",
        "state": "TX",
        "msa": "Houston",
        "county_ids": [ALL_TEXAS_COUNTY_IDS["BRAZORIA"]],
    },
}

ARTICLE_RE = re.compile(r'<article class="lawyer">.*?</article>', re.S)
GIVEN_RE = re.compile(r'given-name">([^<]*)</span>')
FAMILY_RE = re.compile(r'family-name">([^<]*)</span>')
H5_RE = re.compile(r'<h5>([^<]*)</h5>')
ADDR_RE = re.compile(r'class="address">(.*?)</p>', re.S)
PHONE_RE = re.compile(r'tel:([\d\-]+)')
STATUS_RE = re.compile(r'status-icon ([a-z]+ [a-z]+)"')
CONTACTID_RE = re.compile(r'ContactID=(\d+)')
PAGENUM_RE = re.compile(r'PageClick\((\d+),\s*25\)')


def parse_address(raw: str) -> tuple[str, str, str, str]:
    text = re.sub(r"<br\s*/?>", "|", raw)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"<[^>]+>", "", text)
    parts = [p.strip() for p in text.split("|") if p.strip()]
    if not parts:
        return "", "", "", ""
    loc = parts[-1]
    m = re.match(r"^(.+?),\s*([A-Z]{2})\s*(\d{5})?", loc)
    city, state, zipcode = "", "", ""
    if m:
        city = m.group(1).strip()
        state = m.group(2).strip()
        zipcode = (m.group(3) or "").strip()
    street = " ".join(parts[:-1]) if len(parts) > 1 else ""
    return street, city, state, zipcode


def parse_page(html: str) -> list[dict]:
    out = []
    for art in ARTICLE_RE.findall(html):
        fam = FAMILY_RE.search(art)
        if not fam:
            continue
        given_parts = [g.strip() for g in GIVEN_RE.findall(art) if g.strip()]
        name = " ".join(given_parts + [fam.group(1).strip()]).strip()
        if not name:
            continue

        h5 = H5_RE.search(art)
        company = h5.group(1).strip() if h5 else ""

        addr_m = ADDR_RE.search(art)
        street, city, state, zipcode = parse_address(addr_m.group(1)) if addr_m else ("", "", "", "")

        phone_m = PHONE_RE.search(art)
        phone = phone_m.group(1) if phone_m else ""

        status_m = STATUS_RE.search(art)
        status = status_m.group(1) if status_m else ""

        cid_m = CONTACTID_RE.search(art)
        contact_id = cid_m.group(1) if cid_m else ""

        out.append({
            "name": name,
            "company": company,
            "street": street,
            "city": city,
            "state": state,
            "zip": zipcode,
            "phone": phone,
            "status": status,
            "contact_id": contact_id,
        })
    return out


def fetch_county_id(session: requests.Session, county_id: int, delay: float = 0.35) -> list[dict]:
    results = []
    start = 1
    max_page = None
    page = 0
    while True:
        page += 1
        try:
            r = session.post(
                SEARCH_URL,
                data={"Submitted": "1", "Find": "1", "County": str(county_id), "Start": str(start)},
                timeout=30,
            )
        except Exception as e:
            print(f"  [county_id={county_id}] error at start={start}: {e}, retrying in 15s")
            time.sleep(15)
            continue

        if r.status_code == 429:
            print(f"  [county_id={county_id}] rate limited, waiting 60s")
            time.sleep(60)
            continue
        if r.status_code != 200:
            print(f"  [county_id={county_id}] HTTP {r.status_code} at start={start}, stopping")
            break

        entries = parse_page(r.text)
        if not entries:
            break
        results.extend(entries)

        if max_page is None:
            page_nums = [int(n) for n in PAGENUM_RE.findall(r.text)]
            max_page = max(page_nums) if page_nums else page
            print(f"  [county_id={county_id}] {max_page} pages total ({max_page * 25} attorneys, approx)")

        if page >= max_page:
            break
        start += 25
        time.sleep(delay)

        if page % 40 == 0:
            print(f"  [county_id={county_id}] progress: page {page}/{max_page}, {len(results)} attorneys so far")

    print(f"  [county_id={county_id}] done: {len(results)} attorneys")
    return results


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in TEXAS_COUNTIES:
        print(f"Usage: python3 texasbar_discover.py <slug>  (one of {list(TEXAS_COUNTIES)})")
        sys.exit(1)

    slug = sys.argv[1]
    info = TEXAS_COUNTIES[slug]
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = CACHE_DIR / f"{slug}_statebar_cache.json"

    session = requests.Session()
    session.headers.update(HEADERS)

    all_entries = []
    for cid in info["county_ids"]:
        print(f"\n=== {slug}: County id {cid} ===")
        all_entries.extend(fetch_county_id(session, cid))

    cache_path.write_text(json.dumps(all_entries, indent=1))
    print(f"\nWrote {len(all_entries)} raw attorney records to {cache_path}")


if __name__ == "__main__":
    main()
